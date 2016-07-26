/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#include "couchbaseembed.hpp"

#include "platform.h"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"


#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char *g_moduleName = "couchbase";
static const char *g_moduleDescription = "Couchbase Embed Helper";
static const char *g_version = "Couchbase Embed Helper 1.0.0";
static const char *g_compatibleVersions[] = { g_version, nullptr };

COUCHBASEEMBED_PLUGIN_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = g_compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = g_version;
    pb->moduleName = g_moduleName;
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = g_moduleDescription;
    return true;
}

namespace couchbaseembed
{
    //--------------------------------------------------------------------------
    // Plugin Classes
    //--------------------------------------------------------------------------

    CouchbaseRowStream::CouchbaseRowStream(IEngineRowAllocator* resultAllocator, Couchbase::Query * cbaseQuery)
       :   m_CouchBaseQuery(cbaseQuery),
           m_resultAllocator(resultAllocator)
    {
        m_currentRow = 0;
        m_shouldRead = true;

        //iterating over result rows and copying them to stringarray
        //is there a way to independently step through original result rows?
        for (auto cbrow : *m_CouchBaseQuery)
            m_Rows.append(cbrow.json().to_string().c_str());

        if (m_CouchBaseQuery->meta().status().errcode() != LCB_SUCCESS )//rows.length() == 0)
            failx("Embedded couchbase error: %s", m_CouchBaseQuery->meta().body().data());
        else if (m_Rows.length() == 0) // Query errors not reported in meta.status, lets check for errors in meta body
        {
            if (strstr(m_CouchBaseQuery->meta().body().data(), "\"status\": \"errors\""))
                failx("Err: %s", m_CouchBaseQuery->meta().body().data());
        }
    }

    CouchbaseRowStream::~CouchbaseRowStream() {}

    const void * CouchbaseRowStream::nextRow()
    {
        const void * result = NULL;
        if (m_shouldRead && m_currentRow < m_Rows.length())
        {
            auto json = m_Rows.item(m_currentRow++);
            Owned<IPropertyTree> contentTree = createPTreeFromJSONString(json,ipt_caseInsensitive);
            if (contentTree)
            {
                CouchbaseRowBuilder * cbRowBuilder = new CouchbaseRowBuilder(contentTree);
                RtlDynamicRowBuilder rowBuilder(m_resultAllocator);
                const RtlTypeInfo *typeInfo = m_resultAllocator->queryOutputMeta()->queryTypeInfo();
                assertex(typeInfo);
                RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
                size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, *cbRowBuilder);
                return rowBuilder.finalizeRowClear(len);
            }
            else
                failx("Error processing result row");
        }
        return result;
    }

   void CouchbaseRowStream::stop()
   {
       m_resultAllocator.clear();
       m_shouldRead = false;
   }

   Couchbase::Query * CouchbaseConnection::query(Couchbase::QueryCommand * qcommand)
   {
       Couchbase::Status queryStatus;
       Couchbase::Query * pQuery = new Couchbase::Query(*m_pCouchbaseClient, *qcommand, queryStatus);

       if (!queryStatus)
           failx("Couldn't issue query: %s", queryStatus.description());

       if (!pQuery->status())
           failx("Couldn't execute query, reason: %s\nBody is: ", pQuery->meta().body().data());

       if (pQuery->meta().status().errcode() != LCB_SUCCESS )//rows.length() == 0)
           failx("Query execution error: %s", m_pQuery->meta().body().data());

       return pQuery;
   }

    extern void UNSUPPORTED(const char *feature)
    {
        throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in %s", feature, g_version);
    }

    extern void failx(const char *message, ...)
    {
        va_list args;
        va_start(args,message);
        StringBuffer msg;
        msg.appendf("%s: ", g_moduleName).valist_appendf(message,args);
        va_end(args);
        rtlFail(0, msg.str());
    }

    extern void fail(const char *message)
    {
        StringBuffer msg;
        msg.appendf("%s: ", g_moduleName).append(message);
        rtlFail(0, msg.str());
    }

    void bindStringParam(unsigned len, const char *value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            size32_t utf8chars;
            char *utf8;
            rtlStrToUtf8X(utf8chars, utf8, len, value);
            auto status = pQcmd->named_param(cbPlaceholder.str(), utf8);
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), utf8);
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    void bindBoolParam(bool value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            StringBuffer serialized;
            TokenSerializer tokenSerializer;
            tokenSerializer.serialize(value, serialized);

            auto status = pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    void bindDataParam(unsigned len, const void *value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            size32_t bytes;
            void *data;
            rtlStrToDataX(bytes, data, len, value);

            auto status = pQcmd->named_param(cbPlaceholder.str(), (char *)data);
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), (char *)data);
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    void bindIntParam(__int64 value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            StringBuffer serialized;
            TokenSerializer tokenSerializer;
            tokenSerializer.serialize(value, serialized);

            auto status = pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    void bindUIntParam(unsigned __int64 value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            StringBuffer serialized;
            TokenSerializer tokenSerializer;
            tokenSerializer.serialize(value, serialized);

            auto status = pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    void bindRealParam(double value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            StringBuffer serialized;
            TokenSerializer tokenSerializer;
            tokenSerializer.serialize(value, serialized);
            auto status = pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    void bindUnicodeParam(unsigned chars, const UChar *value, const RtlFieldInfo * field, Couchbase::QueryCommand * pQcmd)
    {
        VStringBuffer cbPlaceholder("$%s", field->name->queryStr());
        if (pQcmd)
        {
            size32_t utf8chars;
            char *utf8;
            rtlUnicodeToUtf8X(utf8chars, utf8, chars, value);
            auto status = pQcmd->named_param(cbPlaceholder.str(), utf8);
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), utf8);
        }
        else
            failx("Internal error: detected invalid CouchbaseQueryCommand while attempting to bind to field: %s", cbPlaceholder.str());
    }

    // Bind Couchbase columns from an ECL record
    class CouchbaseRecordBinder : public CInterfaceOf<IFieldProcessor>
    {
    public:
        CouchbaseRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, Couchbase::QueryCommand * _pQcmd, int _firstParam)
         : logctx(_logctx), typeInfo(_typeInfo), m_pQcmd(_pQcmd), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam)
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
            typeInfo->process(row, row, &dummyField, *this); // Bind the variables for the current row
        }

        virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
        {
            checkNextParam(field);
            bindStringParam(len, value, field, m_pQcmd);
        }

        virtual void processBool(bool value, const RtlFieldInfo * field)
        {
            bindBoolParam(value, field, m_pQcmd);
        }

        virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
        {
            bindDataParam(len, value, field, m_pQcmd);
        }

       virtual void processInt(__int64 value, const RtlFieldInfo * field)
       {
           bindIntParam(value, field, m_pQcmd);
       }

       virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
       {
           bindUIntParam(value, field,m_pQcmd);
       }

       virtual void processReal(double value, const RtlFieldInfo * field)
       {
           bindRealParam(value, field, m_pQcmd);
       }

       virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
       {
           Decimal val;
           size32_t bytes;
           rtlDataAttr decText;
           val.setDecimal(digits, precision, value);
           val.getStringX(bytes, decText.refstr());
           processUtf8(bytes, decText.getstr(), field);
       }

       virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
       {
           UNSUPPORTED("UNSIGNED decimals");
       }

       virtual void processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field)
       {
           bindUnicodeParam(chars, value, field, m_pQcmd);
       }

       virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
       {
           size32_t charCount;
           rtlDataAttr text;
           rtlQStrToStrX(charCount, text.refstr(), len, value);
           processUtf8(charCount, text.getstr(), field);
       }

       virtual void processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field)
       {
           bindStringParam(strlen(value), value, field, m_pQcmd);
       }

       virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
       {
           UNSUPPORTED("SET");
           return false;
       }

       virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
       {
           return false;
       }

       virtual bool processBeginRow(const RtlFieldInfo * field)
       {
           return true;
       }

       virtual void processEndSet(const RtlFieldInfo * field)
       {
           UNSUPPORTED("SET");
       }

       virtual void processEndDataset(const RtlFieldInfo * field)
       {
           UNSUPPORTED("DATASET");
       }

       virtual void processEndRow(const RtlFieldInfo * field)
       {
       }

    protected:
       inline unsigned checkNextParam(const RtlFieldInfo * field)
       {
           if (logctx.queryTraceLevel() > 4)
               logctx.CTXLOG("Binding %s to %d", str(field->name), thisParam);
           return thisParam++;
       }

       const RtlTypeInfo *typeInfo;
       Couchbase::QueryCommand * m_pQcmd;
       const IContextLogger &logctx;
       int firstParam;
       RtlFieldStrInfo dummyField;
       int thisParam;
       TokenSerializer m_tokenSerializer;
    };

    class CouchbaseDatasetBinder : public CouchbaseRecordBinder
    {
    public:

        CouchbaseDatasetBinder(const IContextLogger &_logctx, IRowStream * _input, const RtlTypeInfo *_typeInfo, Couchbase::QueryCommand * _pQcmd, int _firstParam)
          : input(_input), CouchbaseRecordBinder(_logctx, _typeInfo, _pQcmd, _firstParam)
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

        void executeAll(CouchbaseConnection * conn)
        {
            while (bindNext())
            {
                auto m_pQuery = conn->query(m_pQcmd);

                if (m_pQuery->meta().status().errcode() != LCB_SUCCESS )//rows.length() == 0)
                    failx("Query execution error: %s", m_pQuery->meta().body().to_string().c_str());

                //consider parsing json result
                if (strstr(m_pQuery->meta().body().data(), "\"status\": \"errors\""))
                    failx("Err: %s", m_pQuery->meta().body().data());
            }
        }

    protected:
        Owned<IRowStream> input;
    };

    // Each call to a Couchbase function will use a new CouchbaseEmbedFunctionContext object
    class CouchbaseEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
    {
    public:
        CouchbaseEmbedFunctionContext(const IContextLogger &_logctx, const char *options, unsigned _flags)
        : logctx(_logctx), m_NextRow(), m_nextParam(0), m_numParams(0), m_scriptFlags(_flags)
        {
            cbQueryIterator = NULL;
            m_pCouchbaseClient = nullptr;
            m_pQuery = nullptr;
            m_pQcmd = nullptr;

            const char *server = "localhost";
            const char *user = "";
            const char *password = "";
            const char *bucketname = "default";
            unsigned port = 8093;
            bool useSSL = false;
            StringBuffer connectionOptions;

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
                        server = val;   // Note that lifetime of val is adequate for this to be safe
                    else if (stricmp(optName, "port")==0)
                        port = atoi(val);
                    else if (stricmp(optName, "user")==0)
                        user = val;
                    else if (stricmp(optName, "password")==0)
                        password = val;
                    else if (stricmp(optName, "bucket")==0)
                        bucketname = val;
                    else if (stricmp(optName, "useSSL")==0)
                        useSSL = clipStrToBool(val);

                    //Connection String options
                    else if (stricmp(optName,   "detailed_errcodes")==0
                            || stricmp(optName, "operation_timeout")==0
                            || stricmp(optName, "config_total_timeout")==0
                            || stricmp(optName, "http_poolsize")==0
                            || stricmp(optName, "detailed_errcodes")==0)
                        connectionOptions.appendf("%s%s=%s", connectionOptions.length() == 0 ? "?" : "&", optName.str(), val);
                    else
                        failx("Unknown option %s", optName.str());
                }
            }

            m_oCBConnection.setown(new CouchbaseConnection(useSSL, server, port, bucketname, user, password, connectionOptions.str()));
            m_oCBConnection->connect();
        }

        IPropertyTree * nextResultRowTree()
        {
            for (auto cbrow : *m_pQuery)
            {
                auto json = cbrow.json().to_string();
                Owned<IPropertyTree> contentTree = createPTreeFromJSONString(json.c_str());
                return contentTree.getLink();
            }
            return nullptr;
        }

        IPropertyTreeIterator * nextResultRowIterator()
        {
            for (auto cbrow : *m_pQuery)
            {
                auto json = cbrow.json().to_string();
                Owned<IPropertyTree> contentTree = createPTreeFromJSONString(json.c_str());
                if (contentTree)
                    return contentTree->getElements("./*");
                failx("Could not fetch next result row.");
                break;
            }
            return nullptr;
        }

        const char * nextResultScalar()
        {
            auto resultrow = nextResultRowIterator();
            if (resultrow)
            {
                resultrow->first();
                if(resultrow->isValid() == true)
                {
                    if (resultrow->query().hasChildren())
                        typeError("scalar", "");
                    return resultrow->query().queryProp("");
                }

                else
                    failx("Could not fetch next result column.");
            }
            else
                failx("Could not fetch next result row.");

            return nullptr;
        }

        virtual bool getBooleanResult()
        {
            bool mybool;
            auto scalar = nextResultScalar();
            handleDeserializeOutcome(m_tokenDeserializer.deserialize(scalar, mybool), "bool", scalar);

            return mybool;
        }

        virtual void getDataResult(size32_t &len, void * &result)
        {
            auto value = nextResultScalar();
            if (value && *value)
            {
                rtlStrToDataX(len, result, strlen(value), value);   // This feels like it may not work to me - will preallocate rather larger than we want
            }
            else
            {
                NullFieldProcessor p(NULL);
                rtlStrToDataX(len, result, p.resultChars, p.stringResult);
            }
        }

        virtual double getRealResult()
        {
            double mydouble;
            auto value = nextResultScalar();
            handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mydouble), "real", value);

            return mydouble;
        }

        virtual __int64 getSignedResult()
        {
            __int64 myint64;
            auto value = nextResultScalar();
            handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myint64), "signed", value);

            return myint64;
        }

        virtual unsigned __int64 getUnsignedResult()
        {
            unsigned __int64 myuint64;
            auto value = nextResultScalar();
            handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myuint64), "unsigned", value);

            return myuint64;
        }

        virtual void getStringResult(size32_t &chars, char * &result)
        {
            auto value = nextResultScalar();
            if (value && *value)
            {
                unsigned numchars = rtlUtf8Length(strlen(value), value);
                rtlUtf8ToStrX(chars, result, numchars, value);
            }

            NullFieldProcessor p(NULL);
            rtlStrToStrX(chars, result, p.resultChars, p.stringResult);
        }

        virtual void getUTF8Result(size32_t &chars, char * &result)
        {
            getStringResult(chars, result);
        }

        virtual void getUnicodeResult(size32_t &chars, UChar * &result)
        {
            auto value = nextResultScalar();
            if (value && *value)
            {
                unsigned numchars = rtlUtf8Length(strlen(value), value);
                rtlUtf8ToUnicodeX(chars, result, numchars, value);
            }

            NullFieldProcessor p(NULL);
            rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
        }

        virtual void getDecimalResult(Decimal &value)
        {
            auto text = nextResultScalar();
            if (text && *text)
                value.setString(rtlUtf8Length(strlen(text), text), text);
            else
            {
                NullFieldProcessor p(NULL);
                value.set(p.decimalResult);
            }
        }

        virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
        {
            UNSUPPORTED("SET results");
        }

        virtual IRowStream * getDatasetResult(IEngineRowAllocator * _resultAllocator)
        {
            Owned<CouchbaseRowStream> cbaseRowStream;
            cbaseRowStream.set(new CouchbaseRowStream(_resultAllocator, m_pQuery));

            return cbaseRowStream.getLink();
        }

        virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
        {
            Owned<CouchbaseRowStream> cbaseRowStream;
            cbaseRowStream.set(new CouchbaseRowStream(_resultAllocator, m_pQuery));

            return (byte *)cbaseRowStream->nextRow();
        }

        virtual size32_t getTransformResult(ARowBuilder & rowBuilder)
        {
            execute();

            auto resultrow = nextResultRowTree();
            if (!resultrow)
                fail("Failed to read row");
            if (resultrow->getCount("./*") != 1)
                typeError("row", "");

            CouchbaseRowBuilder couchbaseRowBuilder(resultrow);
            const RtlTypeInfo *typeInfo = rowBuilder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
            assertex(typeInfo);
            RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
            return typeInfo->build(rowBuilder, 0, &dummyField, couchbaseRowBuilder);
        }

        virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
        {
            CouchbaseRecordBinder binder(logctx, metaVal.queryTypeInfo(), m_pQcmd, m_nextParam);
            binder.processRow(val);
            m_nextParam += binder.numFields();
        }

        virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
        {
            // We only support a single dataset parameter...
            // MORE - look into batch?
            if (m_oInputStream)
            {
                fail("At most one dataset parameter supported");
            }
            m_oInputStream.setown(new CouchbaseDatasetBinder(logctx, LINK(val), metaVal.queryTypeInfo(), m_pQcmd, m_nextParam));
            m_nextParam += m_oInputStream->numFields();
        }

        virtual void bindBooleanParam(const char *name, bool val)
        {
            checkNextParam(name);
            StringBuffer serialized;
            m_tokenSerializer.serialize(val, serialized);

            VStringBuffer cbPlaceholder("$%s", name);
            auto status = m_pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }

        virtual void bindDataParam(const char *name, size32_t len, const void *val)
        {
            checkNextParam(name);
            VStringBuffer cbPlaceholder("$%s", name);
            size32_t bytes;
            void *data;
            rtlStrToDataX(bytes, data, len, val);
            auto status = m_pQcmd->named_param(cbPlaceholder.str(), (char *)data);
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), (char *)data);
        }

        virtual void bindFloatParam(const char *name, float val)
        {
            checkNextParam(name);
            StringBuffer serialized;
            m_tokenSerializer.serialize(val, serialized);
            VStringBuffer cbPlaceholder("$%s", name);

            auto status = m_pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }

        virtual void bindRealParam(const char *name, double val)
        {
            checkNextParam(name);
            StringBuffer serialized;
            m_tokenSerializer.serialize(val, serialized);
            VStringBuffer cbPlaceholder("$%s", name);

            auto status = m_pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }

        virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
        {
            bindSignedParam(name, val);
        }

        virtual void bindSignedParam(const char *name, __int64 val)
        {
            checkNextParam(name);
            StringBuffer serialized;
            m_tokenSerializer.serialize(val, serialized);

            VStringBuffer cbPlaceholder("$%s", name);

            auto status = m_pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }

        virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
        {
            bindUnsignedParam(name, val);
        }

        virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
        {
            checkNextParam(name);
            StringBuffer serialized;
            m_tokenSerializer.serialize(val, serialized);

            VStringBuffer cbPlaceholder("$%s", name);

            auto status = m_pQcmd->named_param(cbPlaceholder.str(), serialized.str());
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), serialized.str());
        }

        virtual void bindStringParam(const char *name, size32_t len, const char *val)
        {
            checkNextParam(name);
            VStringBuffer cbPlaceholder("$%s", name);
            size32_t utf8chars;
            char *utf8;
            rtlStrToUtf8X(utf8chars, utf8, len, val);
            auto status = m_pQcmd->named_param(cbPlaceholder.str(), utf8);
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), utf8);
        }

        virtual void bindVStringParam(const char *name, const char *val)
        {
            checkNextParam(name);
            bindStringParam(name, strlen(val), val);
        }

        virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
        {
            checkNextParam(name);
            bindStringParam(name, strlen(val), val);
        }

        virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
        {
            checkNextParam(name);
            VStringBuffer cbPlaceholder("$%s", name);
            size32_t utf8chars;
            char *utf8;
            rtlUnicodeToUtf8X(utf8chars, utf8, chars, val);
            auto status = m_pQcmd->named_param(cbPlaceholder.str(), utf8);
            if (!status.success())
                failx("Could not bind Param: %s val: %s", cbPlaceholder.str(), utf8);
        }

        virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
        {
            UNSUPPORTED("SET parameters");
        }

        virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
        {
            return NULL;
        }

        virtual void paramWriterCommit(IInterface *writer)
        {
            failx("paramWriterCommit");
        }

        virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
        {
            failx("writeResult");
        }

        virtual void importFunction(size32_t lenChars, const char *text)
        {
            throwUnexpected();
        }

        virtual void compileEmbeddedScript(size32_t chars, const char *script)
        {
            if (script && *script)
            {
                m_pQcmd = new Couchbase::QueryCommand(script);

                if ((m_scriptFlags & EFnoparams) == 0)
                    m_numParams = countBindings(script);
                else
                    m_numParams = 0;
            }
            else
                failx("Empty N1QL query detected");
        }

        virtual void callFunction()
        {
            execute();
        }

    protected:
        void execute()
        {
            if (m_oInputStream)
                m_oInputStream->executeAll(m_oCBConnection);
            else
            {
                m_pQuery = m_oCBConnection->query(m_pQcmd);

                if (m_pQuery->meta().status().errcode() != LCB_SUCCESS )//rows.length() == 0)
                    failx("Query execution error: %s", m_pQuery->meta().body().to_string().c_str());
                if (m_pQuery->status().errcode())
                    failx("Query error: %s", m_pQuery->status().description());

                //consider parsing json result
                if (strstr(m_pQuery->meta().body().to_string().c_str(), "\"status\": \"errors\""))
                    failx("Err: %s", m_pQuery->meta().body().data());
            }
        }

        unsigned countBindings(const char *query)
        {
            unsigned queryCount = 0;
            while ((query = findUnquoted(query, '$')) != NULL)
                queryCount++;
            return queryCount;
        }

        const char * findUnquoted(const char *query, char searchFor)
        {
            // Note - returns pointer to char AFTER the first occurrence of searchFor outside of quotes
            char inStr = '\0';
            char ch;
            while ((ch = *query++) != 0)
            {
                if (ch == inStr)
                    inStr = false;
                else switch (ch)
                {
                case '\'':
                case '"':
                    inStr = ch;
                    break;
                case '\\':
                    if (inStr && *query)
                        query++;
                    break;
                case '/':
                    if (!inStr)
                    {
                        if (*query=='/')
                        {
                            while (*query && *query != '\n')
                                query++;
                        }
                        else if (*query=='*')
                        {
                            query++;
                            loop
                            {
                                if (!*query)
                                    fail("Unterminated comment in query string");
                                if (*query=='*' && query[1]=='/')
                                {
                                    query+= 2;
                                    break;
                                }
                                query++;
                            }
                        }
                    }
                    break;
                default:
                    if (!inStr && ch==searchFor)
                        return query;
                    break;
                }
            }
            return NULL;
        }

        inline unsigned checkNextParam(const char *name)
        {
            if (m_nextParam == m_numParams)
                failx("Too many parameters supplied: No matching $<name> placeholder for parameter %s", name);
            return m_nextParam++;
        }

        const IContextLogger &logctx;
        Owned<CouchbaseConnection>    m_oCBConnection;
        Couchbase::Client           * m_pCouchbaseClient;
        Couchbase::Query            * m_pQuery;
        Couchbase::QueryCommand     * m_pQcmd;

        StringArray m_Rows;
        int m_NextRow;
        Owned<CouchbaseDatasetBinder> m_oInputStream;
        Couchbase::Internal::RowIterator<Couchbase::QueryRow> * cbQueryIterator;
        TokenDeserializer m_tokenDeserializer;
        TokenSerializer m_tokenSerializer;
        unsigned m_nextParam;
        unsigned m_numParams;
        unsigned m_scriptFlags;

    };

    class CouchbaseEmbedContext : public CInterfaceOf<IEmbedContext>
    {
    public:
        virtual IEmbedFunctionContext * createFunctionContext(unsigned flags, const char *options)
        {
            return createFunctionContextEx(NULL, flags, options);
        }

        virtual IEmbedFunctionContext * createFunctionContextEx(ICodeContext * ctx, unsigned flags, const char *options)
        {
            if (flags & EFimport)
            {
                UNSUPPORTED("IMPORT");
                return nullptr;
            }

            else
                return new CouchbaseEmbedFunctionContext(ctx ? ctx->queryContextLogger() : queryDummyContextLogger(), options, flags);
        }

        virtual IEmbedServiceContext * createServiceContext(const char *service, unsigned flags, const char *options)
        {
            throwUnexpected();
            return nullptr;
        }
    };

    extern IEmbedContext* getEmbedContext()
    {
        return new CouchbaseEmbedContext();
    }

    extern bool syntaxCheck(const char *script)
    {
        return true; // TO-DO
    }

} // namespace
