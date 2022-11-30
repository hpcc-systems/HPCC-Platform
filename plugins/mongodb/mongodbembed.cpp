/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#include "mongodbembed.hpp"

#include <map>
#include <mutex>
#include <thread>
#include <cstdlib>
#include <iostream>
#include <string>
#include <memory>
#include <cstdint>

#include "mongocxx/options/client.hpp"
#include "mongocxx/stdx.hpp"
#include "mongocxx/cursor.hpp"
#include "bsoncxx/json.hpp"
#include "bsoncxx/builder/stream/helpers.hpp"
#include "bsoncxx/builder/stream/array_context.hpp"
#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/builder/stream/array.hpp"
#include "bsoncxx/document/value.hpp"
#include "bsoncxx/document/view.hpp"
#include "bsoncxx/stdx/make_unique.hpp"
#include "bsoncxx/stdx/optional.hpp"
#include "bsoncxx/stdx/string_view.hpp"
#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"

#include "platform.h"
#include "jthread.hpp"
#include "rtlembed.hpp"
#include "jptree.hpp"
#include "rtlds_imp.hpp"
#include <time.h>
#include <vector>

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::array;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;
using bsoncxx::builder::stream::key_context;
using bsoncxx::builder::stream::array_context;
using bsoncxx::builder::basic::kvp;

static constexpr const char *MODULE_NAME = "mongodb";
static constexpr const char *MODULE_DESCRIPTION = "MongoDB Embed Helper";
static constexpr const char *VERSION = "MongoDB Embed Helper 1.0.0";
static const char *COMPATIBLE_VERSIONS[] = { VERSION, nullptr };
static const NullFieldProcessor NULLFIELD(NULL);

/**
 * @brief Takes a pointer to an ECLPluginDefinitionBlock and passes in all the important info
 * about the plugin. 
 */
extern "C" MONGODBEMBED_PLUGIN_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx)) 
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = COMPATIBLE_VERSIONS;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = VERSION;
    pb->moduleName = MODULE_NAME;
    pb->ECL = nullptr;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = MODULE_DESCRIPTION;
    return true;
}

// Place example code declarations here for doxygen. Source files should be placed in /mongodb/examples.
/** \example mongodb-test.ecl
 * Here is some example code of the plugin being used.
 */
namespace mongodbembed
{
    const time_t OBJECT_EXPIRE_TIMEOUT_SECONDS = 60 * 2; // Two minutes
    static std::once_flag CONNECTION_CACHE_INIT_FLAG;

    //--------------------------------------------------------------------------
    // Plugin Classes
    //--------------------------------------------------------------------------

    /**
     * @brief Takes an exception object and outputs the error to the user
     * 
     * @param e mongocxx::exception has some values that are useful to look for
     */
    void reportQueryFailure(const mongocxx::exception &e)
    {
        if (e.code() == mongocxx::error_code::k_invalid_collection_object) 
        {
            failx("Collection not found: %s",e.what());
        }
        if (e.code().value() == 26) 
        {
            failx("NamespaceNotFound: %s",e.what());
        }
        if (e.code().value() == 11000) 
        {
            failx("Duplicate Key: %s",e.what());
        }
        if (!isEmptyString(e.what()))
        {
            failx("Exception: %s", e.what());
        }
    }

    /**
     * @brief Helper method for converting an Extended JSON structure into standard JSON
     * 
     * @param result Result string for appending results
     * @param start Pointer to beginning of structure
     * @param row Pointer to the last place where there was standard JSON
     * @param lastBrkt Pointer to the begining of the EJSON structure
     * @param depth Depth of the structure
     */
    void convertEJSONTypes(std::string &result, const char * &start, const char * &row, const char * &lastBrkt, int &depth) 
    {
        while (*start && *start != '$') 
            start++;
        const char * end = start;
        while (*end && *end != '\"') 
            end++;
        std::string key = std::string(start, end - start); // Get datatype
        result += std::string(row, lastBrkt - row); // Add everything before we went into nested document
        // Some data types are unsupported as they are not straightforward to deserialize
        if (key == "$regularExpression") 
        {
            UNSUPPORTED("Regular Expressions"); // TO DO handle unsupported types by not throwing an exception.
        } 
        else if (key == "$timestamp") 
        {
            while (*end && *end != '}') 
                end++; // Skip over timestamp
            row = ++end;
            start = end;
            result += "\"\"";
        } 
        // Both of these get deserialized to strings and are surround by quotation marks
        else if (key == "$date" || key == "$oid") 
        {
            end++;
            while (*end && *end != '\"') 
                end++; // Move to opposite quotation mark
            // The $date datatype can have a nested $numberLong and this checks for that
            if (*(end+1) == '$') 
            {
                while (*end) 
                {
                    if (*end == '\"') 
                    {
                        if (*(end+1) == ' ' || *(end+1) == ':' || *(end+1) == '$') 
                        {
                            end++;
                        } 
                        else 
                            break;
                    }
                    end++;
                }
                start = ++end;
                while (*end && *end != '\"') 
                    end++;

                result += std::string(start, end - start); // Only add the data inside the quotation marks to result string

                while (*end && *end != '}') 
                    end++; // Get out of both nested documents
                end++;

                while (*end && *end != '}') 
                    end++;
                end++;

                depth--;
                row = end; // Set row to just after the nested document
                start = end; // move start to the next place for parsing
            } 
            else 
            {
                start = end++;
                while (*end && *end != '\"') 
                    end++;

                result += std::string(start, ++end - start); // Only add the data inside the quotation marks to result string

                while (*end && *end != '}') 
                    end++; // Only have to get out of one nested document
                end++;
                depth--;
                row = end; // Set row to just after the nested document
                start = end; // move start to the next place for parsing
            }
        } 
        else if (key == "$numberDouble" || key == "$numberDecimal" || key == "$numberLong") 
        {
            // Since these types all represent numbers we don't want to include quotation marks when adding to string result
            end++;
            while (*end && *end != '\"') 
                end++;

            start = ++end;
            while (*end && *end != '\"') 
                end++;

            result += std::string(start, end++ - start); // Only add the data inside the quotation marks to result string
            while (*end && *end != '}') 
                end++; // Only have to get out of one nested document
            end++;
            depth--;
            row = end;
            start = end;
        }
        else
        {
            failx("EJSON datatype error: '%s' is not supported in the current version.", key.c_str());
        }
    }

    /**
     * @brief deserialize MongoDB Extended JSON relaxed format into normal JSON for the RowStream class
     * Examples of EJSON relaxed format are:
     * "dateField": {"$date":{"$numberLong":"1565546054692"}}
     * "doubleField": {"$numberDouble":"10.5"}
     * For more documentation on EJSON. https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
     * 
     * @param result Reference to a result string where the standard JSON should be written.
     * @param row Pointer to the beginning of the result row.
     */
    void deserializeEJSON(std::string &result, const char * row)
    {
        auto start = row;
        int depth = 0; // Keeps track of depth so we don't exit on wrong '}'
        const char * lastBrkt;

        while (*start && (*start != '}' || depth > 1)) 
        {
            if (*start == '\"')
            {
                // Find end of string ignoring any quotes inside the string. This is done by checking what comes after the string which is not foolproof if someone
                // matches json standard output inside their string.
                while(*start && !(*start == '\"' && (*(start + 1) == ' ' || *(start + 1) == ',') && (*(start + 2) == ':' || *(start + 2) == '}' || *(start + 2) == ']')))
                    start++;
                start++;
            }
            // If we see a document increase the depth
            if (*start == '{') 
            {
                depth++;
                lastBrkt = start; // Keep track of last bracket in case we need to backtrack
                // Look for "{ \"$" to mark the start of a datatype
                if (*(start + 1) == ' ' && *(start + 2) == '\"' && *(start + 3) == '$') 
                { 
                    convertEJSONTypes(result, start, row, lastBrkt, depth); // Since we are looking at an Extended JSON structure convert it to normal JSON
                }
            }
            if (*start == '}')
            {
                depth--;
            }
            start++;
        }
        result += std::string(row, (start + 1) - row); // Add everything that is left to the result string. Row gets moved to the place after the last '$'
    }

    MongoDBRowStream::MongoDBRowStream(IEngineRowAllocator* resultAllocator, std::shared_ptr<MongoDBQuery> _query)
        : m_resultAllocator(resultAllocator)
    {
        m_query = _query;
        m_currentRow = 0;
        m_shouldRead = true;
    }

    MongoDBRowStream::~MongoDBRowStream() 
    {
    }

    /**
     * @brief Builds a result row from the query operation using the MongoDBRowBuilder.
     * 
     * @return const void* If a row was built returns the complete row, and if the result rows
     * are empty or it has reached the end it will return a null pointer.
     */
    const void * MongoDBRowStream::nextRow()
    {
        if (m_shouldRead && m_currentRow < m_query->result()->length()) 
        {
            auto json = m_query->result()->item(m_currentRow++);
            Owned<IPropertyTree> contentTree = createPTreeFromJSONString(json,ipt_caseInsensitive);
            if (contentTree)
            {
                MongoDBRowBuilder mdbRowBuilder(contentTree);
                RtlDynamicRowBuilder rowBuilder(m_resultAllocator);
                const RtlTypeInfo *typeInfo = m_resultAllocator->queryOutputMeta()->queryTypeInfo();
                assertex(typeInfo);
                RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
                size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, mdbRowBuilder);
                return rowBuilder.finalizeRowClear(len);
            }
            else
                failx("Error processing result row");
        }
        return nullptr;
    }

    /**
     * @brief Stops the MongoDBRowStream from reading any more rows. Called by the engine. 
     */
    void MongoDBRowStream::stop()
    {
        m_resultAllocator.clear();
        m_shouldRead = false;
    }

    /**
     * @brief Throws an exception and gets called when an operation that is unsupported is attempted.
     * 
     * @param feature Name of the feature that is currently unsupported.
     */
    extern void UNSUPPORTED(const char *feature)
    {
        throw MakeStringException(-1, "%s UNSUPPORTED feature: %s not supported in %s", MODULE_NAME, feature, VERSION);
    }

    /**
     * @brief Exits the program with a failure code and a message to display.
     * 
     * @param message Message to display.
     * @param ... Takes any number of arguments that can be inserted into the string using %.
     */
    extern void failx(const char *message, ...)
    {
        va_list args;
        va_start(args,message);
        StringBuffer msg;
        msg.appendf("%s: ", MODULE_NAME).valist_appendf(message,args);
        va_end(args);
        rtlFail(0, msg.str());
    }

    /**
     * @brief Exits the program with a failure code and a message to display.
     * 
     * @param message Message to display.
     */
    extern void fail(const char *message)
    {
        StringBuffer msg;
        msg.appendf("%s: ", MODULE_NAME).append(message);
        rtlFail(0, msg.str());
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param len Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context and here we grab
     * name of the parameter.
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindStringParam(unsigned len, const char *value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        query->build()->append(kvp(std::string(field->name), std::string(value, len)));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context. 
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindBoolParam(bool value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        query->build()->append(kvp(std::string(field->name), bsoncxx::types::b_bool{value}));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param len Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context. 
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindDataParam(unsigned len, const void *value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        size32_t bytes;
        rtlDataAttr data;
        rtlStrToDataX(bytes, data.refdata(), len, value);

        query->build()->append(kvp(std::string(field->name), std::string(data.getstr(), bytes)));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context. 
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindIntParam(__int64 value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        query->build()->append(kvp(std::string(field->name), bsoncxx::types::b_int64{value}));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context.
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindUIntParam(unsigned __int64 value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        int64_t mVal = value;
        query->build()->append(kvp(std::string(field->name), bsoncxx::types::b_int64{mVal}));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context.
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindRealParam(double value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        query->build()->append(kvp(std::string(field->name), bsoncxx::types::b_double{value}));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param chars Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFielInfo holds a lot of information about the embed context.
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindUnicodeParam(unsigned chars, const UChar *value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8, chars, value);
        query->build()->append(kvp(std::string(field->name), bsoncxx::types::b_utf8{utf8}));
    }

    /**
     * @brief Appends the key value pair to the document. This is used for inserting values into the
     * query string that gets parsed into a bson document.
     * 
     * @param value Decimal value represented as a string.
     * @param field RtlFielInfo holds a lot of information about the embed context.
     * @param query Object holding the bsoncxx::builder::basic::document.
     */
    void bindDecimalParam(std::string value, const RtlFieldInfo * field, std::shared_ptr<MongoDBQuery> query)
    {
        query->build()->append(kvp(std::string(field->name), bsoncxx::types::b_decimal128{value}));
    }

    /**
     * @brief Counts the number of fields in the typeInfo object. This method is used to help bind a Dataset
     * param to a bson document.
     * 
     * @return int Count of fields in Record Structure.
     */
    int MongoDBRecordBinder::numFields()
    {
        int count = 0;
        const RtlFieldInfo * const *fields = typeInfo->queryFields();
        assertex(fields);
        while (*fields++) 
            count++;
        return count;
    }

    /**
     * @brief Calls the typeInfo member function process to bind an ECL row to bson.
     * 
     * @param row Pointer to ECL row.
     */
    void MongoDBRecordBinder::processRow(const byte *row)
    {
        thisParam = firstParam;
        typeInfo->process(row, row, &dummyField, *this);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param len Number of chars in value.
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        checkNextParam(field);
        bindStringParam(len, value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processBool(bool value, const RtlFieldInfo * field)
    {
        bindBoolParam(value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param len Number of chars in value.
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        bindDataParam(len, value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processInt(__int64 value, const RtlFieldInfo * field)
    {
        bindIntParam(value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        bindUIntParam(value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processReal(double value, const RtlFieldInfo * field)
    {
        bindRealParam(value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be bound to bson.
     * @param digits Number of digits in decimal.
     * @param precision Number of digits of precision.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        size32_t bytes;
        rtlDataAttr decText;
        val.setDecimal(digits, precision, value);
        val.getStringX(bytes, decText.refstr());
        bindDecimalParam(decText.getstr(), field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param chars Number of chars in the value.
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field)
    {
        bindUnicodeParam(chars, value, field, query);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param len Length of QString
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processUtf8(charCount, text.getstr(), field);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param chars Number of chars in the value.
     * @param value Data to be bound to bson.
     * @param field Object with information about the current field.
     */
    void MongoDBRecordBinder::processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field)
    {
        bindStringParam(strlen(value), value, field, query);
    }

    /**
     * @brief Checks the next param in the Record.
     * 
     * @param field Object with information about the current field.
     * @return unsigned Index keeping track of current parameter.
     */
    unsigned MongoDBRecordBinder::checkNextParam(const RtlFieldInfo * field)
    {
       if (logctx.queryTraceLevel() > 4) logctx.CTXLOG("Binding %s to %d", field->name, thisParam);
       return thisParam++;
    }

    /**
     * @brief Creates a MongoDBRecordBinder objects and starts processing the row
     * 
     * @param name Name of the Row. Not necessarily useful here.
     * @param metaVal Information about the types in the row.
     * @param val Pointer to the row for processing.
     */
    void MongoDBEmbedFunctionContext::bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val)
    {
        MongoDBRecordBinder binder(logctx, metaVal.queryTypeInfo(), query, m_nextParam);
        binder.processRow(val);
        m_nextParam += binder.numFields();
    }

    /**
     * @brief Creates a MongoDBDatasetBinder object and starts processing the dataset.
     * 
     * @param name Name of the Dataset. Not necessarily useful here.
     * @param metaVal Information about the types in the dataset.
     * @param val Pointer to dataset stream for processing.
     */
    void MongoDBEmbedFunctionContext::bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        if (m_oInputStream) 
        {
            fail("At most one dataset parameter supported");
        }
        m_oInputStream.setown(new MongoDBDatasetBinder(logctx, LINK(val), metaVal.queryTypeInfo(), query, m_nextParam));
        m_nextParam += m_oInputStream->numFields();
    }

    /**
     * @brief Binds an ECL Data param to a bsoncxx::types::b_utf8
     * 
     * @param name Name of the parameter.
     * @param len Length of the value.
     * @param val Pointer to data for binding.
     */
    void MongoDBEmbedFunctionContext::bindDataParam(const char *name, size32_t len, const void *val)
    {
        checkNextParam(name);
        size32_t bytes;
        rtlDataAttr data;
        rtlStrToDataX(bytes, data.refdata(), len, val);

        bindUTF8Param(name, bytes, data.getstr());
    }

    /**
     * @brief Binds an ECL Boolean param to a bsoncxx::types::b_bool.
     * 
     * @param name Name of the parameter.
     * @param val Boolean value.
     */
    void MongoDBEmbedFunctionContext::bindBooleanParam(const char *name, bool val)
    {
        checkNextParam(name);
        std::string terminatedString = name;

        query->build()->append(kvp(terminatedString, bsoncxx::types::b_bool{val}));
    }

    /**
     * @brief Binds an ECL Size param to a bsoncxx::types::b_int64.
     * 
     * @param name Name of the parameter.
     * @param size Size of the value.
     * @param val Integer value.
     */
    void MongoDBEmbedFunctionContext::bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        bindSignedParam(name, val);
    }
    
    /**
     * @brief Binds an ECL Unsigned Size param to a bsoncxx::types::b_int64.
     * 
     * @param name Name of the parameter.
     * @param size Size of the value.
     * @param val Integer value.
     */
    void MongoDBEmbedFunctionContext::bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        bindUnsignedParam(name, val);
    }
     
    /**
     * @brief Binds an ECL Real4 param to a bsoncxx::types::b_double.
     * 
     * @param name Name of the parameter.
     * @param val float value.
     */
    void MongoDBEmbedFunctionContext::bindFloatParam(const char *name, float val)
    {
        checkNextParam(name);
        query->build()->append(kvp(std::string(name), bsoncxx::types::b_double{val}));
    }
     
    /**
     * @brief Binds an ECL Real param to a bsoncxx::types::b_double.
     * 
     * @param name Name of the parameter.
     * @param val Double value.
     */
    void MongoDBEmbedFunctionContext::bindRealParam(const char *name, double val)
    {
        checkNextParam(name);
        query->build()->append(kvp(std::string(name), bsoncxx::types::b_double{val}));
    }
    
    /**
     * @brief Binds an ECL Integer param to a bsoncxx::types::b_int64.
     * 
     * @param name Name of the parameter.
     * @param val Signed Integer value.
     */
    void MongoDBEmbedFunctionContext::bindSignedParam(const char *name, __int64 val)
    {
        checkNextParam(name);
        int64_t mongoVal = val;
        std::string terminatedString = name;

        query->build()->append(kvp(terminatedString, bsoncxx::types::b_int64{mongoVal}));
    }

    /**
     * @brief Binds an ECL Unsigned Integer param to a bsoncxx::types::b_int64.
     * 
     * @param name Name of the parameter.
     * @param val Unsigned Integer value.
     */
    void MongoDBEmbedFunctionContext::bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        checkNextParam(name);
        int64_t mongoVal = val;

        query->build()->append(kvp(std::string(name), bsoncxx::types::b_int64{mongoVal}));
    }
     
    /**
     * @brief Binds an ECL String param to a bsoncxx::types::b_utf8.
     * 
     * @param name Name of the parameter.
     * @param len Number of chars in string.
     * @param val String value.
     */
    void MongoDBEmbedFunctionContext::bindStringParam(const char *name, size32_t len, const char *val)
    {
        checkNextParam(name);
        size32_t utf8Chars;
        rtlDataAttr utf8;
        rtlStrToUtf8X(utf8Chars, utf8.refstr(), len, val);

        query->build()->append(kvp(std::string(name), bsoncxx::types::b_utf8{std::string(utf8.getstr(), rtlUtf8Size(utf8Chars, utf8.getdata()))}));
    }
     
    /**
     * @brief Binds an ECL VString param to a bsoncxx::types::b_utf8.
     * 
     * @param name Name of the parameter.
     * @param val VString value.
     */
    void MongoDBEmbedFunctionContext::bindVStringParam(const char *name, const char *val)
    {
        checkNextParam(name);
        size32_t utf8Chars;
        rtlDataAttr utf8;
        rtlStrToUtf8X(utf8Chars, utf8.refstr(), strlen(val), val);

        query->build()->append(kvp(std::string(name), bsoncxx::types::b_utf8{std::string(utf8.getstr(), rtlUtf8Size(utf8Chars, utf8.getdata()))}));
    }

    /**
     * @brief Binds an ECL UTF8 param to a bsoncxx::types::b_utf8.
     * 
     * @param name Name of the parameter.
     * @param chars Number of chars in string.
     * @param val UTF8 value.
     */
    void MongoDBEmbedFunctionContext::bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        checkNextParam(name);

        query->build()->append(kvp(std::string(name), bsoncxx::types::b_utf8{std::string(val, rtlUtf8Size(chars, val))}));
    }

    /**
     * @brief Binds an ECL Unicode param to a bsoncxx::types::b_utf8.
     * 
     * @param name Name of the parameter.
     * @param chars Number of chars in string.
     * @param val Unicode value.
     */
    void MongoDBEmbedFunctionContext::bindUnicodeParam(const char *name, size32_t chars, const UChar *val) 
    {
        checkNextParam(name);
        size32_t utf8chars;
        rtlDataAttr utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8.refstr(), chars, val);

        query->build()->append(kvp(std::string(name), bsoncxx::types::b_utf8{std::string(utf8.getstr(), rtlUtf8Size(utf8chars, utf8.getdata()))}));
    }

    /**
     * @brief Configures a mongocxx::instance allowing for multiple threads to use it for making connections.
     * The instance is accessed through the MongoDBConnection class.
     */
    static void configure() 
    {
        class noop_logger : public mongocxx::logger 
        {
            public:
                virtual void operator()(mongocxx::log_level,
                            bsoncxx::stdx::string_view,
                            bsoncxx::stdx::string_view) noexcept {}
        };

        auto instance = bsoncxx::stdx::make_unique<mongocxx::instance>(bsoncxx::stdx::make_unique<noop_logger>());

        MongoDBConnection::instance().configure(std::move(instance));
    }

    /**
     * @brief Construct a new MongoDBEmbedFunctionContext object
     * 
     * @param _logctx Context logger for use with the MongoDBRecordBinder
     * MongoDBDatasetBinder classes.
     * @param options Pointer to the list of options that are passed into the Embed function.
     * This contains the server name, username, password, etc.
     * @param _flags Should be zero if the embedded script is ok.
     */
    MongoDBEmbedFunctionContext::MongoDBEmbedFunctionContext(const IContextLogger &_logctx, const char *options, unsigned _flags)
    : logctx(_logctx), m_NextRow(), m_nextParam(0), m_numParams(0), m_scriptFlags(_flags)
    {
        // User options
        const char *server = "";
        const char *user = "";
        const char *password = "";
        const char *databaseName = "";
        const char *collectionName = "";
        const char *connectionOptions = "";
        unsigned port = 0;
        unsigned batchSize = 100;
        StringBuffer connectionString;

        // Iterate over the options from the user
        StringArray inputOptions;
        inputOptions.appendList(options, ",");
        ForEachItemIn(idx, inputOptions) 
        {
            const char *opt = inputOptions.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
                val++;
                if (stricmp(optName, "server")==0)
                    server = val;
                else if (stricmp(optName, "port")==0)
                    port = atoi(val);
                else if (stricmp(optName, "user")==0)
                    user = val;
                else if (stricmp(optName, "password")==0)
                    password = val;
                else if (stricmp(optName, "database")==0)
                    databaseName = val;
                else if (stricmp(optName, "collection")==0)
                    collectionName = val;
                else if (stricmp(optName, "batchSize")==0)
                    batchSize = atoi(val);
                else if (stricmp(optName, "connectionOptions") ==0)
                    connectionOptions = val;
                else
                    failx("Unknown option %s", optName.str());
            }
        }

        // A user cannot supply both a server and port. All MongoDB Atlas clusters use the same port.
        if (!isEmptyString(server) && port !=0)
        {
            failx("Specifying a port is not allowed when connecting to an Atlas Cluster.");
        }
        else if (port != 0)
        {
            if (isEmptyString(user) && isEmptyString(password))
            {
                connectionString.appendf("mongodb://localhost:%i/", port); // Connection to a local MongoDB instance
                if(!isEmptyString(connectionOptions))
                    connectionString.appendf("?%s", connectionOptions);
            }
            else
                failx("Username and Password is not allowed when connecting to local mongod instance");
        }
        else if (!isEmptyString(server))
        {
            if (!isEmptyString(user) && !isEmptyString(password))
            {
                connectionString.appendf("mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority", user, password, server); // Connection to MongoDB Atlas server
                if(!isEmptyString(connectionOptions))
                    connectionString.appendf("&%s", connectionOptions);
            }
            else 
                failx("Username or Password not supplied. Use the user() or password() options in the EMBED declaration.");
        }
        else
        {
            failx("A Server or Port must be supplied in order to connect to MongoDB. Use the server() or port() option to specify the connection type. More information can be found in the README.md file on the plugin github page.");
        }
        std::shared_ptr<MongoDBQuery> ptr(new MongoDBQuery(databaseName, collectionName, connectionString, batchSize));
        query = ptr;

        std::call_once(CONNECTION_CACHE_INIT_FLAG, configure);
        m_oMDBConnection->instance().create_connection(connectionString.str()); 
    }

    /**
     * @brief Destroy the MongoDBEmbedFunctionContext object.
     */
    MongoDBEmbedFunctionContext::~MongoDBEmbedFunctionContext()
    {
    }

    /**
     * @brief Builds an ECL dataset from the result documents of MongoDB query
     * 
     * @param _resultAllocator Used for building the ECL dataset by the engine.
     * @return IRowStream* Stream to ECL dataset handed back to the engine.
     */
    IRowStream * MongoDBEmbedFunctionContext::getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        Owned<MongoDBRowStream> mdbRowStream;
        mdbRowStream.setown(new MongoDBRowStream(_resultAllocator, query));
        return mdbRowStream.getLink();
    }

    /**
     * @brief Builds an ECL row from the result documents of MongoDB query
     * 
     * @param _resultAllocator Used for building the ECL row by the engine.
     * @return byte* Pointer to ECL row handed back to the engine.
     */
    byte * MongoDBEmbedFunctionContext::getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        Owned<MongoDBRowStream> mdbRowStream;
        mdbRowStream.setown(new MongoDBRowStream(_resultAllocator, query));
        return (byte *)mdbRowStream->nextRow();
    }

    // MongoDB does not return scalar values and both the order of documents and fields in those documents are not guarenteed.
    // Therefore the ECL user can have no expectation of what the output will be.
    size32_t MongoDBEmbedFunctionContext::getTransformResult(ARowBuilder & rowBuilder)
    {
        UNIMPLEMENTED_X("MongoDB Transform Result");
        return 0;
    }

    bool MongoDBEmbedFunctionContext::getBooleanResult() 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type BOOLEAN");
        return false;
    }
    
    __int64 MongoDBEmbedFunctionContext::getSignedResult() 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type SIGNED INTEGER");
        return 0;
    }
    
    unsigned __int64 MongoDBEmbedFunctionContext::getUnsignedResult() 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type UNSIGNED INTEGER");
        return 0;
    }

    void MongoDBEmbedFunctionContext::getDataResult(size32_t &len, void * &result) 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type DATA");
    }
    
    double MongoDBEmbedFunctionContext::getRealResult() 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type REAL");
        return 0.0;
    }
    
    void MongoDBEmbedFunctionContext::getStringResult(size32_t &chars, char * &result) 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type STRING");
    }
    
    void MongoDBEmbedFunctionContext::getUTF8Result(size32_t &chars, char * &result) 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type UTF8");
    }
    
    void MongoDBEmbedFunctionContext::getUnicodeResult(size32_t &chars, UChar * &result) 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type UNICODE");
    }
    
    void MongoDBEmbedFunctionContext::getDecimalResult(Decimal &value) 
    {
        UNIMPLEMENTED_X("MongoDB Scalar Return type DECIMAL");
    }

    /**
     * @brief Compiles the embedded script and stores it in the MongoDBQuery object.
     * 
     * @param chars Length of the embedded script.
     * @param script Pointer to the script.
     */
    void MongoDBEmbedFunctionContext::compileEmbeddedScript(size32_t chars, const char *script)
    {
        if (script && *script) 
        {
            // Incoming script is not necessarily null terminated. Note that the chars refers to utf8 characters and not bytes.
            size32_t size = rtlUtf8Size(chars, script);

            if (size > 0) 
            {
                StringAttr queryScript;
                queryScript.set(script, size);
                query->setEmbed(queryScript.get()); // Now null terminated

                if ((m_scriptFlags & EFnoparams) == 0)
                    m_numParams = countParameterPlaceholders(query->script());
                else
                    m_numParams = 0;
            }
            else
                failx("Empty query detected");
        }
        else
            failx("Empty query detected");
    }

    /**
     * @brief Checks the type of a MongoDB element and inserts the key and value using the document builder.
     * 
     * @tparam T bsoncxx::stream::builder::document or bsoncxx::stream::builder::key_context
     * @param builder Context for streaming elements into
     * @param param Key of the pair.
     * @param ele Value that needs to be checked for type before inserting.
     */
    template<typename T>
    void insertValue(T &builder, std::string param, const bsoncxx::document::element& ele)
    {
        if (ele.type() == bsoncxx::type::k_int64) 
        {
            builder << param << ele.get_int64().value;
        } 
        else if (ele.type() == bsoncxx::type::k_bool) 
        {
            builder << param << ele.get_bool().value;
        } 
        else if (ele.type() == bsoncxx::type::k_decimal128) 
        {
            builder << param << ele.get_decimal128().value;
        } 
        else if (ele.type() == bsoncxx::type::k_double) 
        {
            builder << param << ele.get_double().value;
        } 
        else if (ele.type() == bsoncxx::type::k_utf8) 
        {
            builder << param << ele.get_utf8().value;
        } 
        else 
        {
            failx("Error retrieving bound value. Result not built.");
        } 
    }

    /**
     * @brief Checks the type of a MongoDB element and inserts the value into the array_context.
     * 
     * @tparam T bsoncxx::stream::builder::array or bsoncxx::stream::builder::array_context
     * @param ctx Context for streaming elements into
     * @param ele Value that needs to be checked for type before inserting.
     */
    template<typename T>
    void insertValueArr(T &ctx, const bsoncxx::document::element& ele)
    {
        if (ele.type() == bsoncxx::type::k_int64) 
        {
            ctx << ele.get_int64().value;
        } 
        else if (ele.type() == bsoncxx::type::k_bool) 
        {
            ctx << ele.get_bool().value;
        } 
        else if (ele.type() == bsoncxx::type::k_decimal128) 
        {
            ctx << ele.get_decimal128().value;
        } 
        else if (ele.type() == bsoncxx::type::k_double) 
        {
            ctx << ele.get_double().value;
        } 
        else if (ele.type() == bsoncxx::type::k_utf8) 
        {
            ctx << ele.get_utf8().value;
        } 
        else 
        {
            failx("Error retrieving bound value. Array not appended.");
        } 
    }

    /**
     * @brief Checks the document for a particular key.
     * 
     * @param param The param is coming from the script and will be prefixed
     * with a '$', but it will not be stored in the document with the '$'.
     * @param view View of the document that is to be searched.
     * @return true if the value is found in the document.
     * @return false if the value is not in the document.
     */
    bool checkDoc(std::string& param, const bsoncxx::document::view &view)
    {
        if (param[0] == '$') 
        {
            return view.find(param.substr(1)) != view.end();
        }
        return false;
    }

    /**
     * @brief Helper method for checking whether the param or value are stored in view
     * 
     * @tparam T bsoncxx::stream::builder::document or bsoncxx::stream::builder::key_context
     * @param builder Context for streaming elements into
     * @param view Document View for looking up bound parameters from the function definition.
     * @param key must be type string
     * @param isRsvd True if the key is for MongoDB or to look up in view
     * @param value Either the key for getting the value from view or a string to be inserted
     */
    template<typename T>
    void insertPair(T &builder, const bsoncxx::document::view &view, const std::string &key, bool isRsvd, std::string value)
    {
        if (checkDoc(value, view)) 
        {
            if (isRsvd)
            {
                insertValue(builder, key, view[value.substr(1)]);
            } 
            else 
            {
                if (view[key].type() == bsoncxx::type::k_utf8) 
                {
                    insertValue(builder, std::string(view[key].get_utf8().value), view[value.substr(1)]);
                }
            }
        } 
        else 
        {
            if (isRsvd) 
            {
                builder << key << value;
            } 
            else 
            {
                builder << view[key].get_utf8().value << value;
            }
        }
    }

    /**
     * @brief Builds an array object that can hold any number of elements including documents and other arrays.
     * 
     * @tparam T Either a bsoncxx::stream::builder::array or a bsoncxx::stream::builder::array_context
     * @param ctx Context object for streaming array elements into. 
     * @param view Document View for looking up bound parameters from the function definition.
     * @param start The point in the embedded script to start parsing.
     */
    template<typename T>
    void MongoDBEmbedFunctionContext::buildArray(T &ctx, const bsoncxx::document::view &view, const char*& start)
    {
        std::string findStr = " ,(){}[]\t\n\"";
        const char *end;

        while (*start) 
        {
            if (findStr.find(*start) == std::string::npos) 
            {
                end = start + 1;
                while (*end && *end != ',' && *end != ' ' && *end != ']') 
                    end++;
                auto param = std::string(start, end - start);
                bool isRsvd = !checkDoc(param, view);
                if (!isRsvd) 
                {
                    param = param.substr(1);
                    insertValueArr(ctx, view[param]);
                }
                else 
                    ctx << param;
                start = end;
            }
            // Open subdocument
            if (*start == '{')  
            {
                ctx << open_document << [&](key_context<> kctx) {
                    buildDocument(kctx, view, ++start);
                } << close_document;
            } 
            // Open subarray
            if (*start == '[')  
            {
                ctx << open_array << [&](array_context<> actx) {
                    buildArray(actx, view, ++start);
                } << close_array;
            }
            if (*start == ']') 
            {
                start++;
                break;
            } 
            else 
                start++;
        }
    }

    /**
     * @brief Helper function for streaming Key Value pairs into a builder object. It is called on document and
     * key_context objects. Array_context objects don't need Key Value pairs and can take any number of elements.
     * 
     * @tparam T Either a bsoncxx::stream::builder::document or bsoncxx::stream::builder::key_context
     * @param builder Builder object for streaming elements into.
     * @param view Document View for looking up bound parameters from the function definition.
     * @param start The point in the embedded script to start parsing.
     */
    template<typename T>
    void MongoDBEmbedFunctionContext::bindKVP(T &builder, const bsoncxx::document::view &view, const char *&start)
    {
        const char *end = start + 1;
        while (*end && *end != ':' && *end != '\"' && *end != ' ') 
            end++; // pointer to end of key
        auto key = std::string(start, end - start);
        bool isRsvd = !checkDoc(key, view); // If this is a parameter of the function we need to set a flag
        // Remove "$" for the key if it's in view
        if (!isRsvd) 
            key = key.substr(1); 

        start = end + 1;
        while (*start && (*start == ' ' || *start == ':')) 
            start++;

        if (*start == '\"') 
        {
            end = ++start;
            while (*end && *end != '\"') 
                end++;
            insertPair(builder, view, key, isRsvd, std::string(start, end - start));
            start = end + 1;
        } 
        else if (*start == '$') 
        {
            end = start + 1;
            while (*end && *end != ',' && *end != '}' && *end != '\n' && *end != ' ') 
                end++;
            insertPair(builder, view, key, isRsvd, std::string(start, end - start));
            start = end;
        } 
        // Open subdocument
        else if (*start == '{') 
        {
            if (isRsvd) 
            {
                builder << key << open_document << [&](key_context<> ctx) {
                    buildDocument(ctx, view, ++start);
                } << close_document;
            } 
            else 
            {
                if (view[key].type() == bsoncxx::type::k_utf8) 
                {
                    builder << key << open_document << [&](key_context<> ctx) {
                        buildDocument(ctx, view, ++start);
                    } << close_document;
                } 
                else 
                    failx("Key must be type String.");
            }
        }
        // Open subarray
        else if (*start == '[') 
        { 
            if (!isRsvd) 
            {
                if (view[key].type() == bsoncxx::type::k_utf8)
                    key = std::string{view[key].get_utf8().value};
                else 
                    failx("Key must be type String.");
            }
            builder << key << open_array << [&](array_context<> ctx) {
                buildArray(ctx, view, ++start);
            } << close_array;
        } 
    }

    /**
     * @brief Builds a MongoDB document by parsing the embedded script.
     * 
     * @tparam T Can take any object of the type bsoncxx::stream::builder. Only documents,
     * key_contexts, and array_contexts are passed in.
     * @param builder Object to stream elements into.
     * @param view Document view for looking up bound parameters from the function definition.
     * @param start The point in the embedded script to start parsing.
     */
    template<typename T>
    void MongoDBEmbedFunctionContext::buildDocument(T &builder, const bsoncxx::document::view &view, const char *&start)
    {
        std::string findStr = " ,(){}[]\t\n\""; // Characters that do not start a key

        while (*start)
        {
            // If key is found bind pair
            if (findStr.find(*start) == std::string::npos) 
                bindKVP(builder, view, start);
            if (*start == '}' || *start == ';') 
            {
                start++;
                break;
            } 
            else 
                start++;
        }
    }

    /**
     * @brief Creates a MongoDB pipeline from the document builder. A pipeline must be of the form
     * [<stage>, ...] where each stage is a MongoDB document.
     * 
     * @param view Document view that holds the bound parameters from the function definition.
     * @param stages pipeline that holds the stages for the aggregation.
     * @param start Pointer reference to the current place in the embedded script.
     */
    void MongoDBEmbedFunctionContext::buildPipeline(const bsoncxx::document::view &view, mongocxx::pipeline &stages, const char *&start)
    {
        while (*start && *start != '[') 
            start++;
        auto builder = document{};

        while (*start && *start != ';') 
        {
            if (*start == '{') 
            {
                buildDocument(builder, view, ++start); // buildDocument will bring start to the ending "}" of the document
                bsoncxx::document::value doc_value = builder << finalize;
                stages.append_stage(doc_value.view());
                builder.clear();
            }
            start++;
        }
    }

    /**
     * @brief Builds insert arguments and inserts a document.
     * 
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     */
    void MongoDBEmbedFunctionContext::mdbinsert(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value)
    {
        auto builder = document{}; // Bson stream builder
        buildDocument(builder, view, query->script()); // Build document for inserting
        doc_value = builder << finalize; // finalize returns a document::value
        view = doc_value.view();

        try 
        {
            coll.insert_one(view); // Inserts one MongoDB document into coll, the collection chosen by the URI
        } 
        catch (const mongocxx::exception& e) 
        {
            reportQueryFailure(e);
        }
    }

    /**
     * @brief Builds find arguments and finds all documents matching a filter. If find_one is called only one document will be returned.
     * 
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     * @param com Command used to check whether we should call find_one or find.
     */
    void MongoDBEmbedFunctionContext::mdbfind(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value, std::string com)
    {
        auto builder = document{};
        auto start = query->script();
        buildDocument(builder, view, start); // Build filter document
        auto filter_value = builder << finalize;
        auto filter_view = filter_value.view();

        if (com == "find_one") 
        {
            bsoncxx::stdx::optional<bsoncxx::document::value> doc;
            try 
            {
                doc = coll.find_one(filter_view); // Returns single document
            } 
            catch (const mongocxx::exception& e) 
            {
                reportQueryFailure(e);
            }
            if (doc) 
            {
                std::string deserialized;
                deserializeEJSON(deserialized, bsoncxx::to_json(doc->view(), bsoncxx::ExtendedJsonMode::k_relaxed).c_str()); // Deserialize result row
                query->result()->append(deserialized.c_str());
            }
        } 
        else 
        {
            try 
            {
                mongocxx::options::find opts{};
                if (query->size() != 0)
                    opts.batch_size(query->size()); // Batch size default is 100 and is set by user in MongoDBEmbedFunctionContext constructor
                while (*start && *start == ' ') 
                    start++; // Move past whitespace if there is any
                // if there is a comma then we have a projection to build
                if (*start == ',') 
                {
                    auto projection = document{};
                    buildDocument(projection, view, start);
                    doc_value = projection << finalize;
                    view = doc_value.view();
                    opts.projection(view); // Add projection to options
                }
                mongocxx::cursor cursor = coll.find(filter_view, opts); // Get result documents and append them to the row StringArray

                for (auto&& doc : cursor) 
                {
                    std::string deserialized;
                    deserializeEJSON(deserialized, bsoncxx::to_json(doc, bsoncxx::ExtendedJsonMode::k_relaxed).c_str()); 
                    query->result()->append(deserialized.c_str());
                } 
            } 
            catch (const mongocxx::exception& e)
            {
                reportQueryFailure(e);
            }
        }
    }

    /**
     * @brief Builds aggregate arguments and runs the aggregate on a collection.
     * 
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     */
    void MongoDBEmbedFunctionContext::mdbaggregate(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value)
    {
        mongocxx::pipeline stages; 
        auto start = query->script();
        buildPipeline(view, stages, start); // Builds a document for each stage and appends it to the pipeline

        try 
        {
            mongocxx::options::aggregate opts{};
            if (query->size() != 0)
                opts.batch_size(query->size()); // Batch size from user input.
            mongocxx::cursor cursor = coll.aggregate(stages, opts); // Returns a cursor object of documents

            for (auto&& doc : cursor) 
            {
                std::string deserialized;
                deserializeEJSON(deserialized, bsoncxx::to_json(doc, bsoncxx::ExtendedJsonMode::k_relaxed).c_str());
                query->result()->append(deserialized.c_str());
            }
        } 
        catch (const mongocxx::exception& e) 
        {
            reportQueryFailure(e);
        }
    }

    /**
     * @brief Builds a document and runs a command on a MongoDB database.
     * 
     * @param db MongoDB Database for running the command on.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     */
    void MongoDBEmbedFunctionContext::mdbrunCommand(mongocxx::database &db, bsoncxx::document::view &view, bsoncxx::document::value &doc_value)
    {
        auto builder = document{};
        auto start = query->script(); 
        buildDocument(builder, view, start);
        doc_value = builder << finalize;
        view = doc_value.view();

        try 
        {
            bsoncxx::document::value doc = db.run_command(view); // Returns a single document with operation specific output
            
            if (doc.view()["ok"].get_double() == double{1}) 
            {
                std::string deserialized;
                deserializeEJSON(deserialized, bsoncxx::to_json(doc.view()["value"].get_document().view(), bsoncxx::ExtendedJsonMode::k_relaxed).c_str());
                query->result()->append(deserialized.c_str());
            }
        } 
        catch (const mongocxx::operation_exception& e)
        {
            failx("runcommand Error: %s",e.what());
        }
    }

    /**
     * @brief Creates a mongodb Index for searching a collection. Takes either one or two documents.
     * 
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     */
    void MongoDBEmbedFunctionContext::mdbcreateIndex(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value)
    {
        auto builder = document{};
        auto start = query->script();
        buildDocument(builder, view, start);

        while (*start && *start == ' ') 
            start++; // Remove Whitespace
        // If there is a comma then we have an options document to build
        if (*start == ',') 
        {
            auto options = document{};
            buildDocument(options, view, start);

            auto keys_val = builder << finalize;
            auto options_val = options << finalize;

            try 
            {
                coll.create_index(keys_val.view(), options_val.view());
            } 
            catch (const mongocxx::exception& e) 
            {
                reportQueryFailure(e);
            } 
        } 
        else 
        {
            auto keys_val = builder << finalize;

            try 
            {
                coll.create_index(keys_val.view());
            } 
            catch (const mongocxx::exception& e) 
            {
                reportQueryFailure(e);
            } 
        }
    }

    /**
     * @brief Builds a document and runs either delete_one or delete_many on a collection.
     * 
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     * @param com Command used to decide which delete function to run.
     */
    void MongoDBEmbedFunctionContext::mdbdelete(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value, std::string com)
    {
        auto builder = document{};
        bsoncxx::stdx::optional<mongocxx::result::delete_result> result;
        auto start = query->script();
        buildDocument(builder, view, start); // Build delete filter
        doc_value = builder << finalize;
        view = doc_value.view();

        if (com == "delete_one") 
        {
            try 
            {
                result = coll.delete_one(view);
            }
            catch (const mongocxx::exception& e) 
            {
                reportQueryFailure(e);
            }               
        } 
        else 
        {
            try 
            {
                result = coll.delete_many(view);
            }
            catch (const mongocxx::exception& e) 
            {
                reportQueryFailure(e);
            }
        }
        if (result) 
        {
            StringBuffer json;
            json.appendf("{ \"deleted_count\" : %i }", result->deleted_count());
            query->result()->append(json.str());
        }
    }

    /**
     * @brief Builds a document and updates one or many documents based on the filter.
     * 
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     * @param com Command used to decide which update function to run.
     */
    void MongoDBEmbedFunctionContext::mdbupdate(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value, std::string com)
    {
        // takes either (bsoncxx::document, bsoncxx::document) or (bsoncxx::document, mongocxx::pipeline)
        auto builder = document{};
        bsoncxx::stdx::optional<mongocxx::result::update> result;
        auto start = query->script(); 
        buildDocument(builder, view, start); // Build filter document
        auto filter_value = builder << finalize;
        auto filter_view = filter_value.view();

        while (*start && (*start == ',' || *start == ' '))
            start++; // Move to next argument
        // Look for document ('{'), a pipeline ('['), or an error
        if (*start == '{') 
        {
            builder.clear();
            buildDocument(builder, view, ++start);

            auto update_value = builder << finalize;
            auto update_view = update_value.view();
            if (com == "update_one") 
            {
                try 
                {
                    result = coll.update_one(filter_view, update_view); // Returns an update object with counts of affected documents
                } 
                catch (const mongocxx::exception& e) 
                {
                    reportQueryFailure(e);
                }
            } 
            else 
            { 
                try 
                {
                    result = coll.update_many(filter_view, update_view);
                } 
                catch (const mongocxx::exception& e) 
                {
                    reportQueryFailure(e);
                }
            }
        } 
        else if (*start == '[') 
        {
            mongocxx::pipeline stages; 
            buildPipeline(view, stages, start); // Builds a document for each stage and appends it to the pipeline
            if (com == "update_one") 
            {
                try 
                {
                    result = coll.update_one(filter_view, stages);
                } 
                catch (const mongocxx::exception& e) 
                {
                    reportQueryFailure(e);
                } 
            } 
            else 
            {
                try 
                {
                    result = coll.update_many(filter_view, stages);
                } 
                catch (const mongocxx::exception& e) 
                {
                    reportQueryFailure(e);
                }
            }
        } 
        else 
            failx("Incorrect Arguments given to update(). Expected: (bsoncxx::document, bsoncxx::document), (bsoncxx::document, mongocxx::pipeline).");
        // Check if there is a result document and extract useful fields 
        if (result) 
        {
            StringBuffer json;
            json.appendf("{ \"matched_count\" : %i, \"modified_count\" : %i }", result->matched_count(), result->modified_count());
            query->result()->append(json.str());
        }
    }

    /**
     * @brief Helper function for deciding which query to run based on the command.
     * 
     * @param db MongoDB database for the runCommand function.
     * @param coll MongoDB collection to do insert into.
     * @param view View of document where stored params are.
     * @param doc_value Document value gets passed in so we don't have to make a new one.
     */
    void MongoDBEmbedFunctionContext::runQuery(mongocxx::database &db, mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value) 
    {
        std::string com = query->cmd();

        // Handle multiple MongoDB Operations
        if (com == "insert") 
        {
            mdbinsert(coll, view, doc_value);
        } 
        // Returns the first document found that matches the filter
        else if (com == "find_one" || com == "find") 
        {
            mdbfind(coll, view, doc_value, com);
        } 
        else if (com == "update_one" || com == "update_many") 
        {
            mdbupdate(coll, view, doc_value, com);
        }
        // Takes a MongoDB document and can run various commands at a database level. 
        else if (com == "runCommand") 
        {
            mdbrunCommand(db, view, doc_value);
        }
        // Takes a MongoDB pipeline with various stages for chaining commands together 
        else if (com == "aggregate") 
        {
            mdbaggregate(coll, view, doc_value);
        } 
        else if (com == "delete_one" || com == "delete_many") 
        {
            mdbdelete(coll, view, doc_value, com);
        } 
        else if (com == "create_index") 
        {
            mdbcreateIndex(coll, view, doc_value);
        } 
        else 
        {
            StringBuffer err;
            err.appendf("Unsupported operation: %s", com.c_str());
            UNSUPPORTED(err.str());
        }
    }
    
    /**
     * @brief Calls the execute function
     * 
     */
    void MongoDBEmbedFunctionContext::callFunction()
    {
        execute();
    }

    /**
     * @brief If a dataset or row was passed in it called executeAll otherwise it gets 
     * a connection and runs the query using the embedded script.
     * 
     */
    void MongoDBEmbedFunctionContext::execute()
    {
        if (m_oInputStream)
            m_oInputStream->executeAll(m_oMDBConnection);
        else 
        {
            // Get a MongoDB instance from the connection object
            auto conn = m_oMDBConnection->instance().get_connection(query->uri());
            mongocxx::database db = (*conn)[query->database()];
            mongocxx::collection coll = db[query->collection()];

            bsoncxx::document::value doc_value = query->build()->extract();
            bsoncxx::document::view view = doc_value.view();

            runQuery(db, coll, view, doc_value);
        }
    }

    /**
     * @brief Checks the next param to see if it was passed in.
     * 
     * @param name Parameter name
     * @return unsigned Index of next parameter to check.
     */
    unsigned MongoDBEmbedFunctionContext::checkNextParam(const char *name)
    {
        if (m_nextParam == m_numParams)
            failx("Too many parameters supplied: No matching $<name> placeholder for parameter %s", name);
        return m_nextParam++;
    }

    /**
     * @brief Gets a Boolean result for an ECL Row
     * 
     * @param field Holds the value of the field.
     * @return bool Returns the boolean value from the result row. 
     */
    bool MongoDBRowBuilder::getBooleanResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            return p.boolResult;
        }

        bool mybool;
        mongodbembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mybool), "bool", value);
        return mybool;
    }

    /**
     * @brief Gets a data result from the result row and passes it back to engine through result.
     * 
     * @param field Holds the value of the field.
     * @param len Length of the Data value.
     * @param result Used for returning the result to the caller.
     */
    void MongoDBRowBuilder::getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlStrToDataX(len, result, p.resultChars, p.stringResult);
            return;
        }
        rtlStrToDataX(len, result, strlen(value), value); // This feels like it may not work to me - will preallocate rather larger than we want
    }

    /**
     * @brief Gets a real result from the result row.
     * 
     * @param field Holds the value of the field.
     * @return double Double value to return.
     */
    double MongoDBRowBuilder::getRealResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            return p.doubleResult;
        }

        double mydouble = 0.0;
        mongodbembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mydouble), "real", value);
        return mydouble;
    }

    /**
     * @brief Gets the Signed Integer result from the result row.
     * 
     * @param field Holds the value of the field.
     * @return __int64 Value to return.
     */
    __int64 MongoDBRowBuilder::getSignedResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);
        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            return p.uintResult;
        }

        __int64 myint64 = 0;
        mongodbembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myint64), "signed", value);
        return myint64;
    }

    /**
     * @brief Gets the Unsigned Integer result from the result row.
     * 
     * @param field Holds the value of the field.
     * @return unsigned Value to return.
     */
    unsigned __int64 MongoDBRowBuilder::getUnsignedResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);
        if (!value || !*value) 
        {

            NullFieldProcessor p(field);
            return p.uintResult;
        }

        unsigned __int64 myuint64 = 0;
        mongodbembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myuint64), "unsigned", value);
        return myuint64;
    }

    /**
     * @brief Gets a String from the result row.
     * 
     * @param field Holds the value of the field.
     * @param chars Number of chars in the String.
     * @param result Variable used for returning string back to the caller.
     */
    void MongoDBRowBuilder::getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlUtf8ToStrX(chars, result, p.resultChars, p.stringResult);
            return;
        }

        unsigned numchars = rtlUtf8Length(strlen(value), value);
        rtlUtf8ToStrX(chars, result, numchars, value);
        return;
    }

    /**
     * @brief Gets a UTF8 from the result row.
     * 
     * @param field Holds the value of the field.
     * @param chars Number of chars in the UTF8.
     * @param result Variable used for returning UTF8 back to the caller.
     */
    void MongoDBRowBuilder::getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlUtf8ToUtf8X(chars, result, p.resultChars, p.stringResult);
            return;
        }

        unsigned numchars = rtlUtf8Length(strlen(value), value);
        rtlUtf8ToUtf8X(chars, result, numchars, value);
        return;
    }

    /**
     * @brief Gets a Unicode from the result row.
     * 
     * @param field Holds the value of the field.
     * @param chars Number of chars in the Unicode.
     * @param result Variable used for returning Unicode back to the caller.
     */
    void MongoDBRowBuilder::getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
            return;
        }

        unsigned numchars = rtlUtf8Length(strlen(value), value); // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
        rtlUtf8ToUnicodeX(chars, result, numchars, value);
        return;
    }

    /**
     * @brief Gets a decimal from the result row.
     * 
     * @param field Holds the value of the field.
     * @param value Variable used for returning decimal to caller.
     */
    void MongoDBRowBuilder::getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        const char * dvalue = nextField(field);
        if (!dvalue || !*dvalue) 
        {
            NullFieldProcessor p(field);
            value.set(p.decimalResult);
            return;
        }

        size32_t chars;
        rtlDataAttr result;
        value.setString(strlen(dvalue), dvalue);
        RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *) field->type;
        value.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
    }

    /**
     * @brief Starts a new Set.
     * 
     * @param field Field with information about the context of the set.
     * @param isAll Not Supported.
     */
    void MongoDBRowBuilder::processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        isAll = false; // ALL not supported

        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            PathTracker newPathNode(xpath, CPNTSet);
            StringBuffer newXPath;

            constructNewXPath(newXPath, xpath.str());

            newPathNode.childCount = m_oResultRow->getCount(newXPath);
            m_pathStack.push_back(newPathNode);
        } 
        else 
        {
            failx("processBeginSet: Field name or xpath missing");
        }
    }

    /**
     * @brief Checks if we should process another set.
     * 
     * @param field Context information about the set.
     * @return true If the children that we have process is less than the total child count.
     * @return false If all the children sets have been processed.
     */
    bool MongoDBRowBuilder::processNextSet(const RtlFieldInfo * field)
    {
        return m_pathStack.back().childrenProcessed < m_pathStack.back().childCount;
    }

    /**
     * @brief Starts a new Dataset.
     * 
     * @param field Information about the context of the dataset.
     */
    void MongoDBRowBuilder::processBeginDataset(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            PathTracker newPathNode(xpath, CPNTDataset);
            StringBuffer newXPath;

            constructNewXPath(newXPath, xpath.str());

            newPathNode.childCount = m_oResultRow->getCount(newXPath);
            m_pathStack.push_back(newPathNode);
        } 
        else 
        {
            failx("processBeginDataset: Field name or xpath missing");
        }
    }

    /**
     * @brief Starts a new Row.
     * 
     * @param field Information about the context of the row.
     */
    void MongoDBRowBuilder::processBeginRow(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            if (strncmp(xpath.str(), "<nested row>", 12) == 0) 
            {
                // Row within child dataset
                if (m_pathStack.back().nodeType == CPNTDataset) 
                {
                    m_pathStack.back().currentChildIndex++;
                } 
                else 
                {
                    failx("<nested row> received with no outer dataset designated");
                }
            } 
            else 
            {
                m_pathStack.push_back(PathTracker(xpath, CPNTScalar));
            }
        } 
        else 
        {
            failx("processBeginRow: Field name or xpath missing");
        }
    }

    /**
     * @brief Checks whether we should process the next row.
     * 
     * @param field Information about the context of the row.
     * @return true If the number of child rows process is less than the total count of children.
     * @return false If all of the child rows have been processed.
     */
    bool MongoDBRowBuilder::processNextRow(const RtlFieldInfo * field)
    {
        return m_pathStack.back().childrenProcessed < m_pathStack.back().childCount;
    }

    /**
     * @brief Ends a set.
     * 
     * @param field Information about the context of the set.
     */
    void MongoDBRowBuilder::processEndSet(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty() && !m_pathStack.empty() && strcmp(xpath.str(), m_pathStack.back().nodeName.str()) == 0) 
        {
            m_pathStack.pop_back();
        }
    }

    /**
     * @brief Ends a dataset.
     * 
     * @param field Information about the context of the dataset.
     */
    void MongoDBRowBuilder::processEndDataset(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            if (!m_pathStack.empty() && strcmp(xpath.str(), m_pathStack.back().nodeName.str()) == 0) 
            {
                m_pathStack.pop_back();
            }
        } 
        else 
        {
            failx("processEndDataset: Field name or xpath missing");
        }
    }

    /**
     * @brief Ends a row.
     * 
     * @param field Information about the context of the row.
     */
    void MongoDBRowBuilder::processEndRow(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            if (!m_pathStack.empty()) 
            {
                if (m_pathStack.back().nodeType == CPNTDataset) 
                {
                    m_pathStack.back().childrenProcessed++;
                } 
                else if (strcmp(xpath.str(), m_pathStack.back().nodeName.str()) == 0) 
                {
                    m_pathStack.pop_back();
                }
            }
        } 
        else 
        {
            failx("processEndRow: Field name or xpath missing");
        }
    }

    /**
     * @brief Gets the next field and processes it.
     * 
     * @param field Information about the context of the next field.
     * @return const char* Result of building field.
     */
    const char * MongoDBRowBuilder::nextField(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (xpath.isEmpty()) 
        {
            failx("nextField: Field name or xpath missing");
        }
        StringBuffer fullXPath;

        if (!m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet && strncmp(xpath.str(), "<set element>", 13) == 0) 
        {
            m_pathStack.back().currentChildIndex++;
            constructNewXPath(fullXPath, NULL);
            m_pathStack.back().childrenProcessed++;
        } 
        else 
        {
            constructNewXPath(fullXPath, xpath.str());
        }

        return m_oResultRow->queryProp(fullXPath.str());
    }

    void MongoDBRowBuilder::xpathOrName(StringBuffer & outXPath, const RtlFieldInfo * field) const
    {
        outXPath.clear();

        if (field->xpath) 
        {
            if (field->xpath[0] == xpathCompoundSeparatorChar) 
            {
                outXPath.append(field->xpath + 1);
            } 
            else 
            {
                const char * sep = strchr(field->xpath, xpathCompoundSeparatorChar);

                if (!sep) 
                {
                    outXPath.append(field->xpath);
                } 
                else 
                {
                    outXPath.append(field->xpath, 0, static_cast<size32_t>(sep - field->xpath));
                }
            }
        } 
        else 
        {
            outXPath.append(field->name);
        }
    }

    void MongoDBRowBuilder::constructNewXPath(StringBuffer& outXPath, const char * nextNode) const
    {
        bool nextNodeIsFromRoot = (nextNode && *nextNode == '/');

        outXPath.clear();

        if (!nextNodeIsFromRoot) 
        {
            // Build up full parent xpath using our previous components
            for (std::vector<PathTracker>::const_iterator iter = m_pathStack.begin(); iter != m_pathStack.end(); iter++) 
            {
                if (strncmp(iter->nodeName, "<row>", 5) != 0) 
                {
                    if (!outXPath.isEmpty()) 
                    {
                        outXPath.append("/");
                    }
                    outXPath.append(iter->nodeName);
                    if (iter->nodeType == CPNTDataset || iter->nodeType == CPNTSet) 
                    {
                        outXPath.appendf("[%d]", iter->currentChildIndex);
                    }
                }
            }
        }

        if (nextNode && *nextNode) 
        {
            if (!outXPath.isEmpty()) 
            {
                outXPath.append("/");
            }
            outXPath.append(nextNode);
        }
    }

    /**
     * @brief Serves as the entry point for the HPCC Engine into the plugin and is how it obtains a 
     * MongoDBEmbedFunctionContext object for creating the query and executing it.
     * 
     */
    class MongoDBEmbedContext : public CInterfaceOf<IEmbedContext>
    {
    public:
        virtual IEmbedFunctionContext * createFunctionContext(unsigned flags, const char *options) override
        {
            return createFunctionContextEx(nullptr, nullptr, flags, options);
        }

        virtual IEmbedFunctionContext * createFunctionContextEx(ICodeContext * ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
        {
            if (flags & EFimport) 
            {
                UNSUPPORTED("IMPORT");
                return nullptr;
            } 
            else 
                return new MongoDBEmbedFunctionContext(ctx ? ctx->queryContextLogger() : queryDummyContextLogger(), options, flags);
        }

        virtual IEmbedServiceContext * createServiceContext(const char *service, unsigned flags, const char *options) override
        {
            throwUnexpected();
            return nullptr;
        }
    };


    extern DECL_EXPORT IEmbedContext* getEmbedContext()
    {
        return new MongoDBEmbedContext();
    }

    extern DECL_EXPORT bool syntaxCheck(const char *script)
    {
        return true; // TO-DO
    }
} // namespace

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
}
