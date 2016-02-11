/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
#include "mysql.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlembed.hpp"
#include "roxiemem.hpp"
#include "nbcd.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif


static void UNSUPPORTED(const char *feature) __attribute__((noreturn));

static void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in mysql plugin", feature);
}

static const char * compatibleVersions[] = {
    "MySQL Embed Helper 1.0.0",
    NULL };

static const char *version = "MySQL Embed Helper 1.0.0";

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
    pb->moduleName = "mysql";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "MySQL Embed Helper";
    return true;
}

namespace mysqlembed {

static void failx(const char *msg, ...) __attribute__((noreturn))  __attribute__((format(printf, 1, 2)));
static void fail(const char *msg) __attribute__((noreturn));

static void failx(const char *message, ...)
{
    va_list args;
    va_start(args,message);
    StringBuffer msg;
    msg.append("mysql: ").valist_appendf(message,args);
    va_end(args);
    rtlFail(0, msg.str());
}

static void fail(const char *message)
{
    StringBuffer msg;
    msg.append("mysql: ").append(message);
    rtlFail(0, msg.str());
}


// Wrappers to MySQL structures that require corresponding releases

class MySQLConnection : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    MySQLConnection(MYSQL *_conn) : conn(_conn)
    {
    }
    ~MySQLConnection()
    {
        if (conn)
            mysql_close(conn);
    }
    inline operator MYSQL *() const
    {
        return conn;
    }
private:
    MySQLConnection(const MySQLConnection &);
    MYSQL *conn;
};

class MySQLResult : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    MySQLResult(MYSQL_RES *_res) : res(_res)
    {
    }
    ~MySQLResult()
    {
        if (res)
            mysql_free_result(res);
    }
    inline operator MYSQL_RES *() const
    {
        return res;
    }
private:
    MySQLResult(const MySQLResult &);
    MYSQL_RES *res;
};

class MySQLStatement : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    MySQLStatement(MYSQL_STMT *_stmt) : stmt(_stmt)
    {
    }
    ~MySQLStatement()
    {
        if (stmt)
            mysql_stmt_close(stmt);
    }
    inline operator MYSQL_STMT *() const
    {
        return stmt;
    }
private:
    MySQLStatement(const MySQLStatement &);
    MYSQL_STMT *stmt;
};

class MySQLBindingArray
{
public:
    MySQLBindingArray()
    {
        columns = 0;
        bindinfo = NULL;
        is_null = NULL;;
        error = NULL;
        lengths = NULL;
    }
    void init(unsigned count)
    {
        columns = count;
        if (columns)
        {
            bindinfo = new MYSQL_BIND [columns];
            is_null = new my_bool [columns];
            error = new my_bool [columns];
            lengths = new unsigned long [columns];
            memset(bindinfo, 0, columns * sizeof(bindinfo[0]));
            memset(is_null, 0, columns * sizeof(is_null[0]));
            memset(error, 0, columns * sizeof(error[0]));
            memset(lengths, 0, columns * sizeof(lengths[0]));
            for (int i = 0; i < columns; i++)
            {
                bindinfo[i].is_null = &is_null[i];
                bindinfo[i].length = &lengths[i];
                bindinfo[i].error = &error[i];
            }
        }
    }
    void bindResults(MYSQL_RES *res)
    {
        init(mysql_num_fields(res));
        for (int i = 0; i < columns; i++)
        {
            MYSQL_FIELD *col = mysql_fetch_field_direct(res, i);
            switch (col->type)
            {
            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
                bindinfo[i].buffer_type = MYSQL_TYPE_STRING;
                bindinfo[i].buffer_length = 100;  // MORE - is there a better guess?
                break;
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATE:
                bindinfo[i].buffer_type = col->type;
                bindinfo[i].buffer_length = sizeof(MYSQL_TIME);
                break;
            default:
                bindinfo[i].buffer_type = col->type;
                bindinfo[i].buffer_length = col->length;
                break;
            }
            bindinfo[i].buffer = rtlMalloc(bindinfo[i].buffer_length);
        }
    }
    ~MySQLBindingArray()
    {
        for (int i = 0; i < columns; i++)
        {
            rtlFree(bindinfo[i].buffer);
        }
        delete [] bindinfo;
        delete [] is_null;
        delete [] error;
        delete [] lengths;
    }
    inline int numColumns() const
    {
        return columns;
    }
    inline MYSQL_BIND &queryColumn(int colIdx, const char *name) const
    {
        if (colIdx >= columns)
        {
            VStringBuffer error("No matching bound column for parameter %d", colIdx);
            if (name)
                error.appendf(" (%s)", name);
            fail(error);
        }
        return bindinfo[colIdx];
    }
    inline MYSQL_BIND *queryBindings() const
    {
        return bindinfo;
    }
private:
    MYSQL_BIND *bindinfo;
    my_bool *is_null;
    my_bool *error;
    unsigned long *lengths;
    int columns;
};

class MySQLPreparedStatement : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    MySQLPreparedStatement(MySQLConnection *_conn, MySQLStatement *_stmt)
    : conn(_conn), stmt(_stmt)
    {
        // Create bindings for input parameters
        inputBindings.init(mysql_stmt_param_count(*stmt));
        // And for results
        res.setown(new MySQLResult(mysql_stmt_result_metadata(*stmt)));
        if (*res)
        {
            resultBindings.bindResults(*res);
            /* Bind the result buffers */
            if (mysql_stmt_bind_result(*stmt, resultBindings.queryBindings()))
                fail(mysql_stmt_error(*stmt));
        }
        else if (mysql_stmt_errno(*stmt))  // SQL actions don't return results...
            fail(mysql_stmt_error(*stmt));
    }
    ~MySQLPreparedStatement()
    {
        stop();
    }
    inline void stop()
    {
        res.clear();
        stmt.clear();
    }
    bool next()
    {
        if (!stmt)
            return false;
        int rc = mysql_stmt_fetch(*stmt);
        if (rc == MYSQL_NO_DATA)
            return false;
        else if (rc)
            fail(mysql_stmt_error(*stmt));
        else
            return true;
    }
    void execute()
    {
        assertex(stmt && *stmt);
        if (inputBindings.numColumns() && mysql_stmt_bind_param(*stmt, inputBindings.queryBindings()))
            fail(mysql_stmt_error(*stmt));
        if (mysql_stmt_execute(*stmt))
            fail(mysql_stmt_error(*stmt));
    }
    inline const MySQLBindingArray &queryResultBindings() const
    {
        return resultBindings;
    }
    inline const MySQLBindingArray &queryInputBindings() const
    {
        return inputBindings;
    }
    inline bool hasResult() const
    {
        return *res != NULL;
    }
protected:
    Linked<MySQLConnection> conn;
    Linked<MySQLStatement> stmt;
    Owned<MySQLResult> res;
    MySQLBindingArray inputBindings;
    MySQLBindingArray resultBindings;
};

// Conversions from MySQL values to ECL data

static void typeError(const char *expected, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const RtlFieldInfo *field)
{
    VStringBuffer msg("mysql: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", field->name->queryStr());
    rtlFail(0, msg.str());
}

static bool isInteger(enum_field_types type)
{
    switch (type)
    {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
        return true;
    default:
        return false;
    }
}

static bool isDateTime(enum_field_types type)
{
    switch (type)
    {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
        return true;
    default:
        return false;
    }
}

static bool isString(enum_field_types type)
{
    switch (type)
    {
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
        return true;
    default:
        return false;
    }
}


static unsigned __int64 getDateTimeValue(const MYSQL_BIND &bound)
{
    const MYSQL_TIME * time = (const MYSQL_TIME *) bound.buffer;
    switch (bound.buffer_type)
    {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
        //What format should this be?  Possibly a timestamp_t
        return (unsigned __int64)((time->year * 10000) + (time->month * 100) + (time->day)) * 1000000 +
               (time->hour * 10000) + (time->minute * 100) + (time->second);
    case MYSQL_TYPE_DATE:
        return (time->year * 10000) + (time->month * 100) + (time->day);
    case MYSQL_TYPE_TIME:
        return (time->hour * 10000) + (time->minute * 100) + (time->second);
    default:
        throwUnexpected();
    }
}

static void getDateTimeText(const MYSQL_BIND &bound, size32_t &chars, char * &result)
{
    const MYSQL_TIME * time = (const MYSQL_TIME *) bound.buffer;
    char temp[20];
    switch (bound.buffer_type)
    {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
        _snprintf(temp, sizeof(temp), "%4u-%02u-%02u %02u:%02u:%02u", time->year, time->month, time->day, time->hour, time->minute, time->second);
        break;
    case MYSQL_TYPE_DATE:
        _snprintf(temp, sizeof(temp), "%4u-%02u-%02u", time->year, time->month, time->day);
        break;
    case MYSQL_TYPE_TIME:
        _snprintf(temp, sizeof(temp), "%02u:%02u:%02u", time->hour, time->minute, time->second);
        break;
    default:
        throwUnexpected();
    }
    rtlStrToStrX(chars, result, strlen(temp), temp);
}

static bool getBooleanResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.boolResult;
    }
    if (!isInteger(bound.buffer_type))
        typeError("boolean", field);
    return rtlReadUInt(bound.buffer, *bound.length) != 0;
}

static void getDataResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, void * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlStrToDataX(chars, result, p.resultChars, p.stringResult);
        return;
    }
    if (isString(bound.buffer_type))
        rtlStrToDataX(chars, result, *bound.length, bound.buffer);   // This feels like it may not work to me - will preallocate rather larger than we want
    else
        typeError("blob", field);
}

static __int64 getSignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound);
static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound);

static double getRealResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.doubleResult;
    }
    if (isInteger(bound.buffer_type))
    {
        if (bound.is_unsigned)
            return (double) getUnsignedResult(field, bound);
        else
            return (double) getSignedResult(field, bound);
    }
    else if (bound.buffer_type == MYSQL_TYPE_FLOAT)
        return * (float *) bound.buffer;
    else if (bound.buffer_type == MYSQL_TYPE_DOUBLE)
        return * (double *) bound.buffer;
    else
        typeError("double", field);
}

static __int64 getSignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.intResult;
    }
    if (isDateTime(bound.buffer_type))
        return getDateTimeValue(bound);
    if (isInteger(bound.buffer_type))
    {
        if (bound.is_unsigned)
            return (__int64) rtlReadUInt(bound.buffer, *bound.length);
        else
            return rtlReadInt(bound.buffer, *bound.length);
    }
    else
        typeError("integer", field);
}

static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.uintResult;
    }
    if (isDateTime(bound.buffer_type))
        return getDateTimeValue(bound);
    if (!isInteger(bound.buffer_type))
        typeError("integer", field);
    if (bound.is_unsigned)
        return rtlReadUInt(bound.buffer, *bound.length);
    else
        return (unsigned __int64) rtlReadInt(bound.buffer, *bound.length);
}

static void getStringResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, char * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlStrToStrX(chars, result, p.resultChars, p.stringResult);
        return;
    }

    if (isDateTime(bound.buffer_type))
    {
        getDateTimeText(bound, chars, result);
        return;
    }
    if (!isString(bound.buffer_type))
        typeError("string", field);

    const char *text = (const char *) bound.buffer;
    unsigned long bytes = *bound.length;
    unsigned numchars = rtlUtf8Length(bytes, text);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
    rtlUtf8ToStrX(chars, result, numchars, text);
}

static void getUTF8Result(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, char * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlUtf8ToUtf8X(chars, result, p.resultChars, p.stringResult);
        return;
    }

    if (isDateTime(bound.buffer_type))
    {
        getDateTimeText(bound, chars, result);
        return;
    }
    if (!isString(bound.buffer_type))
        typeError("string", field);

    const char *text = (const char *) bound.buffer;
    unsigned long bytes = *bound.length;
    unsigned numchars = rtlUtf8Length(bytes, text);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
    rtlUtf8ToUtf8X(chars, result, numchars, text);
}

static void getUnicodeResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, UChar * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
        return;
    }
    if (bound.buffer_type != MYSQL_TYPE_STRING && bound.buffer_type != MYSQL_TYPE_VAR_STRING)
        typeError("string", field);
    const char *text = (const char *) bound.buffer;
    unsigned long bytes = *bound.length;
    unsigned numchars = rtlUtf8Length(bytes, text);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
    rtlUtf8ToUnicodeX(chars, result, numchars, text);
}

static void getDecimalResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, Decimal &value)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        value.set(p.decimalResult);
        return;
    }
    size32_t chars;
    rtlDataAttr result;
    mysqlembed::getStringResult(field, bound, chars, result.refstr());
    value.setString(chars, result.getstr());
    if (field)
    {
        RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *) field->type;
        value.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
    }
}

static void createBindBuffer(MYSQL_BIND & bindInfo, enum_field_types sqlType, unsigned size)
{
    if (size)
    {
        if (!bindInfo.buffer)
        {
            bindInfo.buffer_type = sqlType;
            bindInfo.buffer = rtlMalloc(size);
        }
        else
            assertex(bindInfo.buffer_type == sqlType);
    }
    else
    {
        // Buffer is reallocated each time - caller is responsible for it.
        bindInfo.buffer_type = sqlType;
        rtlFree(bindInfo.buffer);
        bindInfo.buffer = NULL;
    }
}

// A MySQLRowBuilder object is used to construct an ECL row from a MySQL row

class MySQLRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    MySQLRowBuilder(const MySQLBindingArray &_resultInfo)
    : resultInfo(_resultInfo), colIdx(-1)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getBooleanResult(field, nextField(field));
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        mysqlembed::getDataResult(field, nextField(field), len, result);
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getRealResult(field, nextField(field));
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getSignedResult(field, nextField(field));
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getUnsignedResult(field, nextField(field));
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        mysqlembed::getStringResult(field, nextField(field), chars, result);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        mysqlembed::getUTF8Result(field, nextField(field), chars, result);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        mysqlembed::getUnicodeResult(field, nextField(field), chars, result);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        mysqlembed::getDecimalResult(field, nextField(field), value);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        UNSUPPORTED("SET fields");
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        throwUnexpected();
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
    const MYSQL_BIND &nextField(const RtlFieldInfo * field)
    {
        if (colIdx < resultInfo.numColumns())
            colIdx++;
        else
            fail("Too many fields in ECL output row");
        const MYSQL_BIND &column = resultInfo.queryColumn(colIdx,field->name->queryStr());
        if (*column.error)
            failx("Error fetching column %s", field->name->queryStr());
        return column;
    }
    const MySQLBindingArray &resultInfo;
    int colIdx;
};

// Bind MySQL variables from an ECL record

class MySQLRecordBinder : public CInterfaceOf<IFieldProcessor>
{
public:
    MySQLRecordBinder(const RtlTypeInfo *_typeInfo, const MySQLBindingArray &_bindings, int _firstParam)
      : typeInfo(_typeInfo), bindings(_bindings), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam)
    {
    }
    int numFields()
    {
        int count = 0;
        const RtlFieldInfo * const *fields = typeInfo->queryFields();
        assertex(fields);
        while (*fields++)
            count++;
        return count;
    }
    void processRow(const byte *row)
    {
        thisParam = firstParam;
        typeInfo->process(row, row, &dummyField, *this);   // Bind the variables for the current row
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        char *utf8;
        rtlStrToUtf8X(utf8chars, utf8, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_TINY, sizeof(value));
        * (bool *) bindInfo.buffer = value;
        bindInfo.is_unsigned = true;
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        size32_t bytes;
        void *data;
        rtlStrToDataX(bytes, data, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_BLOB, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_LONGLONG, sizeof(value));
        * (__int64 *) bindInfo.buffer = value;
        bindInfo.is_unsigned = false;
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_LONGLONG, sizeof(value));
        * (unsigned __int64 *) bindInfo.buffer = value;
        bindInfo.is_unsigned = true;
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_DOUBLE, sizeof(value));
        * (double *) bindInfo.buffer = value;
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        size32_t bytes;
        char *data;
        val.setDecimal(digits, precision, value);
        val.getStringX(bytes, data);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        size32_t bytes;
        char *data;
        val.setUDecimal(digits, precision, value);
        val.getStringX(bytes, data);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processString(charCount, text.getstr(), field);
    }
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUtf8ToUtf8X(utf8chars, utf8, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
    {
        UNSUPPORTED("SET fields");
        return false;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        UNSUPPORTED("Nested datasets");
        return false;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        return true;
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
    MYSQL_BIND &createBindBuffer(enum_field_types sqlType, unsigned size)
    {
        MYSQL_BIND &bindInfo = bindings.queryColumn(thisParam++, NULL);
        mysqlembed::createBindBuffer(bindInfo, sqlType, size);
        return bindInfo;
    }
    const RtlTypeInfo *typeInfo;
    const MySQLBindingArray &bindings;
    int firstParam;
    RtlFieldStrInfo dummyField;
    int thisParam;
};

//

class MySQLDatasetBinder : public MySQLRecordBinder
{
public:
    MySQLDatasetBinder(IRowStream * _input, const RtlTypeInfo *_typeInfo, const MySQLBindingArray &_bindings, int _firstParam)
      : input(_input), MySQLRecordBinder(_typeInfo, _bindings, _firstParam)
    {
    }
    bool bindNext()
    {
        roxiemem::OwnedConstRoxieRow nextRow = (const byte *) input->ungroupedNextRow();
        if (!nextRow)
            return false;
        processRow((const byte *) nextRow.get());   // Bind the variables for the current row
        return true;
    }
    void executeAll(MySQLPreparedStatement *stmtInfo)
    {
        while (bindNext())
        {
            stmtInfo->execute();
        }
    }
protected:
    Owned<IRowStream> input;
};

// A MySQL function that returns a dataset will return a MySQLRowStream object that can be
// interrogated to return each row of the result in turn

class MySQLRowStream : public CInterfaceOf<IRowStream>
{
public:
    MySQLRowStream(MySQLDatasetBinder *_inputStream, MySQLPreparedStatement *_stmtInfo, IEngineRowAllocator *_resultAllocator)
    : inputStream(_inputStream), stmtInfo(_stmtInfo), resultAllocator(_resultAllocator)
    {
        executePending = true;
        eof = false;
    }
    virtual const void *nextRow()
    {
        // A little complex when streaming data in as well as out - want to execute for every input record
        if (eof)
            return NULL;
        loop
        {
            if (executePending)
            {
                executePending = false;
                if (inputStream && !inputStream->bindNext())
                {
                    noteEOF();
                    return NULL;
                }
                stmtInfo->execute();
            }
            if (stmtInfo->next())
                break;
            if (inputStream)
                executePending = true;
            else
            {
                noteEOF();
                return NULL;
            }
        }
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        MySQLRowBuilder mysqlRowBuilder(stmtInfo->queryResultBindings());
        const RtlTypeInfo *typeInfo = resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, mysqlRowBuilder);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        resultAllocator.clear();
        stmtInfo->stop();
    }

protected:
    void noteEOF()
    {
        if (!eof)
        {
            eof = true;
            stop();
        }
    }
    Linked<MySQLDatasetBinder> inputStream;
    Linked<MySQLPreparedStatement> stmtInfo;
    Linked<IEngineRowAllocator> resultAllocator;
    bool executePending;
    bool eof;
};

// Each call to a MySQL function will use a new MySQLEmbedFunctionContext object

static __thread ThreadTermFunc threadHookChain;
static __thread MySQLConnection *cachedConnection = NULL;
static __thread const char *cachedServer = NULL;
static __thread const char *cachedUser = NULL;
static __thread const char *cachedPassword = NULL;
static __thread const char *cachedDatabase = NULL;
static __thread unsigned cachedPort = 0;

static bool cachedConnectionMatches(const char *server, unsigned port, const char *user, const char *password, const char *database)
{
    return streq(server, cachedServer) && port==cachedPort && streq(user, cachedUser) && streq(password, cachedPassword) && streq(database, cachedDatabase);
}

static void clearCache()
{
    ::Release(cachedConnection);
    cachedConnection = NULL;
    free((void *) cachedServer);
    free((void *) cachedUser);
    free((void *) cachedPassword);
    free((void *) cachedDatabase);
    cachedServer = cachedUser = cachedPassword = cachedDatabase = NULL;
    cachedPort = 0;
}

static bool mysqlInitialized = false;
static __thread bool mysqlThreadInitialized = false;
static CriticalSection initCrit;

static void terminateMySqlThread()
{
    clearCache();
    mysql_thread_end();
    mysqlThreadInitialized = false;  // In case it was a threadpool thread...
    if (threadHookChain)
    {
        (*threadHookChain)();
        threadHookChain = NULL;
    }
}

static void initializeMySqlThread()
{
    if (!mysqlThreadInitialized)
    {
        {
            CriticalBlock b(initCrit);
            if (!mysqlInitialized)
            {
                mysqlInitialized = true;
                mysql_library_init(0, NULL, NULL);
            }
        }
        mysql_thread_init();
        threadHookChain = addThreadTermFunc(terminateMySqlThread);
        mysqlThreadInitialized = true;
    }
}

static void cacheConnection(MySQLConnection *connection, const char *server, unsigned port, const char *user, const char *password, const char *database)
{
    clearCache();
    cachedServer = strdup(server);
    cachedUser = strdup(user);
    cachedPassword = strdup(password);
    cachedDatabase = strdup(database);
    cachedPort = port;
    cachedConnection = LINK(connection);
}

class MySQLEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    MySQLEmbedFunctionContext(const char *options)
      : nextParam(0)
    {
        const char *server = "localhost";
        const char *user = "";
        const char *password = "";
        const char *database = "";
        bool caching = true;
        unsigned port = 0;
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
                if (stricmp(optName, "server")==0)
                    server = val;   // Note that lifetime of val is adequate for this to be safe
                else if (stricmp(optName, "port")==0)
                    port = atoi(val);
                else if (stricmp(optName, "user")==0)
                    user = val;
                else if (stricmp(optName, "password")==0)
                    password = val;
                else if (stricmp(optName, "database")==0)
                    database = val;
                else if (stricmp(optName, "cache")==0)
                    caching = clipStrToBool(val);
            }
        }
        initializeMySqlThread();
        if (caching && cachedConnection && cachedConnectionMatches(server, port, user, password, database))
        {
            conn.set(cachedConnection);
        }
        else
        {
            if (cachedConnection)
            {
                ::Release(cachedConnection);
                cachedConnection = NULL;
            }
            conn.setown(new MySQLConnection(mysql_init(NULL)));
            if (!mysql_real_connect(*conn, server, user, password, database, port, NULL, 0))
            {
                VStringBuffer err("mysql: failed to connect (%s)", mysql_error(*conn));
                rtlFail(0, err.str());
            }
            if (caching)
            {
                cacheConnection(conn, server, port, user, password, database);
            }
        }
    }
    virtual bool getBooleanResult()
    {
        bool ret = mysqlembed::getBooleanResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual void getDataResult(size32_t &len, void * &result)
    {
        mysqlembed::getDataResult(NULL, getScalarResult(), len, result);
        checkSingleRow();
    }
    virtual double getRealResult()
    {
        double ret = mysqlembed::getRealResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual __int64 getSignedResult()
    {
        __int64 ret = mysqlembed::getSignedResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        unsigned __int64 ret = mysqlembed::getUnsignedResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual void getStringResult(size32_t &chars, char * &result)
    {
        mysqlembed::getStringResult(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getUTF8Result(size32_t &chars, char * &result)
    {
        mysqlembed::getUTF8Result(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getUnicodeResult(size32_t &chars, UChar * &result)
    {
        mysqlembed::getUnicodeResult(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getDecimalResult(Decimal &value)
    {
        mysqlembed::getDecimalResult(NULL, getScalarResult(), value);
        checkSingleRow();
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        UNSUPPORTED("SET results");
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        return new MySQLRowStream(inputStream, stmtInfo, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        if (!stmtInfo->hasResult())
            typeError("row", NULL);
        lazyExecute();
        MySQLRowStream stream(NULL, stmtInfo, _resultAllocator);
        roxiemem::OwnedConstRoxieRow ret = stream.nextRow();
        roxiemem::OwnedConstRoxieRow ret2 = stream.nextRow();
        stream.stop();
        if (ret ==  NULL || ret2 != NULL)  // Check for exactly one returned row
            typeError("row", NULL);
        return (byte *) ret.getClear();
    }
    virtual size32_t getTransformResult(ARowBuilder & rowBuilder)
    {
        if (!stmtInfo->next())
            typeError("row", NULL);
        MySQLRowBuilder mysqlRowBuilder(stmtInfo->queryResultBindings());
        const RtlTypeInfo *typeInfo = rowBuilder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t ret = typeInfo->build(rowBuilder, 0, &dummyField, mysqlRowBuilder);
        if (stmtInfo->next())
            typeError("row", NULL);  // Check that a single row was returned
        return ret;
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
    {
        MySQLRecordBinder binder(metaVal.queryTypeInfo(), stmtInfo->queryInputBindings(), nextParam);
        binder.processRow(val);
        nextParam += binder.numFields();
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        // We only support a single dataset parameter...
        if (inputStream)
        {
            fail("At most one dataset parameter supported");
        }
        inputStream.setown(new MySQLDatasetBinder(LINK(val), metaVal.queryTypeInfo(), stmtInfo->queryInputBindings(), nextParam));
        nextParam += inputStream->numFields();
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_TINY, sizeof(val));
        * (bool *) bindInfo.buffer = val;
        bindInfo.is_unsigned = true;
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        size32_t bytes;
        void *data;
        rtlStrToDataX(bytes, data, len, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_BLOB, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_FLOAT, sizeof(val));
        * (float *) bindInfo.buffer = val;
    }
    virtual void bindRealParam(const char *name, double val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_DOUBLE, sizeof(val));
        * (double *) bindInfo.buffer = val;
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        bindSignedParam(name, val);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_LONGLONG, sizeof(val));
        * (__int64 *) bindInfo.buffer = val;
        bindInfo.is_unsigned = false;
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        bindUnsignedParam(name, val);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_LONGLONG, sizeof(val));
        * (unsigned __int64 *) bindInfo.buffer = val;
        bindInfo.is_unsigned = true;
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlStrToUtf8X(utf8chars, utf8, len, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUtf8ToUtf8X(utf8chars, utf8, chars, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8, chars, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        UNSUPPORTED("SET parameters");  // MySQL does support sets, so MIGHT be possible...
    }
    virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
    {
        return NULL;
    }
    virtual void paramWriterCommit(IInterface *writer)
    {
    }
    virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
    {
    }


    virtual void importFunction(size32_t lenChars, const char *text)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t chars, const char *script)
    {
        size32_t len = rtlUtf8Size(chars, script);
        Owned<MySQLStatement> stmt  = new MySQLStatement(mysql_stmt_init(*conn));
        if (!*stmt)
            fail("failed to create statement");
        if (mysql_stmt_prepare(*stmt, script, len))
            fail(mysql_stmt_error(*stmt));
        stmtInfo.setown(new MySQLPreparedStatement(conn, stmt));
    }
    virtual void callFunction()
    {
        if (nextParam != stmtInfo->queryInputBindings().numColumns())
            failx("Not enough parameters supplied (%d parameters supplied, but statement has %d bound columns)", nextParam, stmtInfo->queryInputBindings().numColumns());
        if (!stmtInfo->hasResult())
            lazyExecute();
    }
protected:
    void lazyExecute()
    {
        if (inputStream)
            inputStream->executeAll(stmtInfo);
        else
            stmtInfo->execute();
    }
    const MYSQL_BIND &getScalarResult()
    {
        if (!stmtInfo->hasResult() || stmtInfo->queryResultBindings().numColumns() != 1)
            typeError("scalar", NULL);
        lazyExecute(); // MORE this seems wrong to me  - or at least needs to check not already executed
        if (!stmtInfo->next())
            typeError("scalar", NULL);
        return stmtInfo->queryResultBindings().queryColumn(0, NULL);
    }
    void checkSingleRow()
    {
        if (stmtInfo->next())
            typeError("scalar", NULL);
    }
    inline MYSQL_BIND &findParameter(const char *name, enum_field_types sqlType, unsigned size)
    {
        // Everything is positional in MySQL
        MYSQL_BIND &bindInfo = stmtInfo->queryInputBindings().queryColumn(nextParam++, name);
        createBindBuffer(bindInfo, sqlType, size);
        return bindInfo;
    }
    Owned<MySQLConnection> conn;
    Owned<MySQLPreparedStatement> stmtInfo;
    Owned<MySQLDatasetBinder> inputStream;
    int nextParam;
};

class MySQLEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options)
    {
        return createFunctionContextEx(NULL, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, unsigned flags, const char *options)
    {
        if (flags & EFimport)
            UNSUPPORTED("IMPORT");
        else
            return new MySQLEmbedFunctionContext(options);
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options)
    {
        throwUnexpected();
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new MySQLEmbedContext();
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
