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

#ifndef _COUCHBASEEMBED_INCL
#define _COUCHBASEEMBED_INCL

#ifdef COUCHBASEEMBED_PLUGIN_EXPORTS
#define COUCHBASEEMBED_PLUGIN_API DECL_EXPORT
#else
#define COUCHBASEEMBED_PLUGIN_API DECL_IMPORT
#endif

//Using cpp wrapper from https://github.com/couchbaselabs/libcouchbase-cxx
#include <cassert>
#include <libcouchbase/couchbase++.h>
#include <libcouchbase/couchbase++/views.h>
#include <libcouchbase/couchbase++/query.h>
#include <libcouchbase/couchbase++/endure.h>
#include <libcouchbase/couchbase++/logging.h>

#include "platform.h"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"
#include "rtlembed.hpp"
#include "jptree.hpp"
#include "tokenserialization.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield.hpp"
#include "roxiemem.hpp"
#include <time.h>

#include <vector>

namespace couchbaseembed
{
    extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
    extern void failx(const char *msg, ...) __attribute__((noreturn))  __attribute__((format(printf, 1, 2)));
    extern void fail(const char *msg) __attribute__((noreturn));

    static void typeError(const char *expected, const char * fieldname)
    {
        VStringBuffer msg("Couchbase: type mismatch - %s expected", expected);
        if (fieldname && *fieldname)
            msg.appendf(" for field %s", fieldname);
        rtlFail(0, msg.str());
    }

    static void typeError(const char *expected, const RtlFieldInfo *field)
    {
        typeError(expected, field ? field->name : nullptr);
    }

    static int getNumFields(const RtlTypeInfo *record)
    {
        int count = 0;
        const RtlFieldInfo * const *fields = record->queryFields();
        assertex(fields);
        while (*fields++)
            count++;
        return count;
    }

    static void handleDeserializeOutcome(DeserializationResult resultcode, const char * targetype, const char * culpritvalue)
    {
        switch (resultcode)
        {
            case Deserialization_SUCCESS:
                break;
            case Deserialization_BAD_TYPE:
                failx("Deserialization error (%s): value cannot be const", targetype);
                break;
            case Deserialization_UNSUPPORTED:
                failx("Deserialization error (%s): encountered value type not supported", targetype);
                break;
            case Deserialization_INVALID_TOKEN:
                failx("Deserialization error (%s): token cannot be NULL, empty, or all whitespace", targetype);
                break;
            case Deserialization_NOT_A_NUMBER:
                failx("Deserialization error (%s): non-numeric characters found in numeric conversion: '%s'", targetype, culpritvalue);
                break;
            case Deserialization_OVERFLOW:
                failx("Deserialization error (%s): number too large to be represented by receiving value", targetype);
                break;
            case Deserialization_UNDERFLOW:
                failx("Deserialization error (%s): number too small to be represented by receiving value", targetype);
                break;
            default:
                typeError(targetype, culpritvalue);
                break;
        }
    }

    static const char * findUnquotedChar(const char *query, char searchFor)
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
                        for (;;)
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

    static unsigned countParameterPlaceholders(const char *query)
    {
        unsigned queryCount = 0;
        while ((query = findUnquotedChar(query, '$')) != NULL)
            queryCount++;
        return queryCount;
    }

    class CouchbaseRowStream : public RtlCInterface, implements IRowStream
    {
    public:
        CouchbaseRowStream(IEngineRowAllocator* _resultAllocator, Couchbase::Query * cbaseQuery);
        virtual ~CouchbaseRowStream();

        RTLIMPLEMENT_IINTERFACE
        virtual const void* nextRow();
        virtual void stop();
    private:
        Linked<IEngineRowAllocator>     m_resultAllocator;  //!< Pointer to allocator used when building result rows
        bool                            m_shouldRead;       //!< If true, we should continue trying to read more messages
        StringArray                     m_Rows;             //!< Local copy of result rows
        __int64                         m_currentRow;       //!< Current result row

    };

    class CouchbaseConnection : public CInterface
    {
    public:
        CouchbaseConnection(bool useSSL, const char * host, unsigned port, const char * bucketname, const char * password, const char * connOptions, const char * user)
        {
            StringBuffer connectionString;

            makeConnectionString(useSSL, host, port, bucketname, connOptions, connectionString);
            m_pCouchbaseClient = new Couchbase::Client(connectionString.str(), password, user);
            timeLastUsed = 0;
        }

        CouchbaseConnection(const StringBuffer& connectionString, const char * password, const char * user)
        {
            m_pCouchbaseClient = new Couchbase::Client(connectionString.str(), password, user);
            timeLastUsed = 0;
        }

        virtual ~CouchbaseConnection()
        {
            if (m_pCouchbaseClient)
            {
                delete m_pCouchbaseClient;
                m_pCouchbaseClient = nullptr;
            }
        }

        static void makeConnectionString(bool useSSL, const char * host, unsigned port, const char * bucketname, const char * connOptions, StringBuffer& out)
        {
            out.setf("couchbase%s://%s:%d/%s%s", useSSL ? "s" : "", host, port, bucketname, connOptions);
        }

        Couchbase::Query * query(Couchbase::QueryCommand * qcommand);

        inline void connect() { m_connectionStatus = m_pCouchbaseClient->connect(); }
        inline const Couchbase::Status& getConnectionStatus() const { return m_connectionStatus; }
        inline time_t getTimeTouched() const { return timeLastUsed; }
        inline void updateTimeTouched() {timeLastUsed = time(NULL); }

    private:
        Couchbase::Client * m_pCouchbaseClient;
        Couchbase::Status  m_connectionStatus;

        CouchbaseConnection(const CouchbaseConnection &);

        time_t timeLastUsed;
    };

    enum PathNodeType {CPNTScalar, CPNTDataset, CPNTSet};

    struct PathTracker
    {
        StringBuffer    nodeName;
        PathNodeType    nodeType;
        unsigned int    currentChildIndex;
        unsigned int    childCount;
        unsigned int    childrenProcessed;

        // Simple constructor
        PathTracker()
        {}

        // Constructor given node name and dataset bool
        PathTracker(const StringBuffer& _nodeName, PathNodeType _nodeType)
            :   nodeName(_nodeName), nodeType(_nodeType), currentChildIndex(0), childCount(0), childrenProcessed(0)
        {}

        // Copy constructor
        PathTracker(const PathTracker& other)
            :   nodeName(other.nodeName), nodeType(other.nodeType), currentChildIndex(other.currentChildIndex), childCount(other.childCount), childrenProcessed(other.childrenProcessed)
        {}
    };

    class CouchbaseRowBuilder : public CInterfaceOf<IFieldSource>
    {
    public:
        CouchbaseRowBuilder(IPropertyTree * resultrow)
        {
            m_oResultRow.set(resultrow);
            if (!m_oResultRow)
                failx("Missing result row data");
            m_pathStack.reserve(10);
        }

        virtual bool getBooleanResult(const RtlFieldInfo *field);
        virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result);
        virtual double getRealResult(const RtlFieldInfo *field);
        virtual __int64 getSignedResult(const RtlFieldInfo *field);
        virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field);
        virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result);
        virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result);
        virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result);
        virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value);
        virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll);
        virtual bool processNextSet(const RtlFieldInfo * field);
        virtual void processBeginDataset(const RtlFieldInfo * field);
        virtual void processBeginRow(const RtlFieldInfo * field);
        virtual bool processNextRow(const RtlFieldInfo * field);
        virtual void processEndSet(const RtlFieldInfo * field);
        virtual void processEndDataset(const RtlFieldInfo * field);
        virtual void processEndRow(const RtlFieldInfo * field);

    protected:
        const char * nextField(const RtlFieldInfo * field);
        void xpathOrName(StringBuffer & outXPath, const RtlFieldInfo * field) const;
        void constructNewXPath(StringBuffer& outXPath, const char * nextNode) const;
    private:
        TokenDeserializer m_tokenDeserializer;
        Owned<IPropertyTree> m_oResultRow;
        std::vector<PathTracker> m_pathStack;
    };

    // Bind Couchbase columns from an ECL record
    class CouchbaseRecordBinder : public CInterfaceOf<IFieldProcessor>
    {
    public:
        CouchbaseRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, Couchbase::QueryCommand * _pQcmd, int _firstParam)
         : typeInfo(_typeInfo), m_pQcmd(_pQcmd), logctx(_logctx), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam) {}

        int numFields();
        void processRow(const byte *row);
        virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field);
        virtual void processBool(bool value, const RtlFieldInfo * field);
        virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field);
        virtual void processInt(__int64 value, const RtlFieldInfo * field);
        virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field);
        virtual void processReal(double value, const RtlFieldInfo * field);
        virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field);
        virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
        {
            UNSUPPORTED("UNSIGNED decimals");
        }

        virtual void processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field);
        virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field);
        virtual void processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field);
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
        inline unsigned checkNextParam(const RtlFieldInfo * field);

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
          : CouchbaseRecordBinder(_logctx, _typeInfo, _pQcmd, _firstParam), input(_input)
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
                std::unique_ptr<Couchbase::Query> query(conn->query(m_pQcmd));

                if (query->meta().status().errcode() != LCB_SUCCESS )//rows.length() == 0)
                    failx("Query execution error: %s", query->meta().body().to_string().c_str());

                //consider parsing json result
                if (strstr(query->meta().body().data(), "\"status\": \"errors\""))
                    failx("Err: %s", query->meta().body().data());
            }
        }

    protected:
        Owned<IRowStream> input;
    };

    class CouchbaseEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
    {
       public:
           CouchbaseEmbedFunctionContext(const IContextLogger &_logctx, const char *options, unsigned _flags);
           virtual ~CouchbaseEmbedFunctionContext();
           IPropertyTree * nextResultRowTree();
           IPropertyTreeIterator * nextResultRowIterator();
           const char * nextResultScalar();
           virtual bool getBooleanResult();
           virtual void getDataResult(size32_t &len, void * &result);
           virtual double getRealResult();
           virtual __int64 getSignedResult();
           virtual unsigned __int64 getUnsignedResult();
           virtual void getStringResult(size32_t &chars, char * &result);
           virtual void getUTF8Result(size32_t &chars, char * &result);
           virtual void getUnicodeResult(size32_t &chars, UChar * &result);
           virtual void getDecimalResult(Decimal &value);
           virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
           {
               UNSUPPORTED("SET results");
           }
           virtual IRowStream * getDatasetResult(IEngineRowAllocator * _resultAllocator);
           virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator);
           virtual size32_t getTransformResult(ARowBuilder & rowBuilder);
           virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val) override;
           virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val);
           virtual void bindBooleanParam(const char *name, bool val);
           virtual void bindDataParam(const char *name, size32_t len, const void *val);
           virtual void bindFloatParam(const char *name, float val);
           virtual void bindRealParam(const char *name, double val);
           virtual void bindSignedSizeParam(const char *name, int size, __int64 val);
           virtual void bindSignedParam(const char *name, __int64 val);
           virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val);
           virtual void bindUnsignedParam(const char *name, unsigned __int64 val);
           virtual void bindStringParam(const char *name, size32_t len, const char *val);
           virtual void bindVStringParam(const char *name, const char *val);
           virtual void bindUTF8Param(const char *name, size32_t chars, const char *val);
           virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val);
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
               UNSUPPORTED("paramWriterCommit");
           }
           virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
           {
               UNSUPPORTED("writeResult");
           }
           virtual void importFunction(size32_t lenChars, const char *text)
           {
               UNSUPPORTED("importFunction");
           }
           virtual void compileEmbeddedScript(size32_t chars, const char *script);
           virtual void callFunction();
           virtual void loadCompiledScript(size32_t chars, const void *_script) override
           {
               UNSUPPORTED("loadCompiledScript");
           }
           virtual void enter() override {}
           virtual void reenter(ICodeContext *codeCtx) override {}
           virtual void exit() override {}
       protected:
           void execute();
           unsigned countBindings(const char *query);
           const char * findUnquoted(const char *query, char searchFor);
           unsigned checkNextParam(const char *name);

           const IContextLogger &logctx;
           CouchbaseConnection         * m_oCBConnection;
           Couchbase::Query            * m_pQuery;
           Couchbase::QueryCommand     * m_pQcmd;
           Owned<IPropertyTreeIterator>  m_resultrow;

           StringArray m_Rows;
           int m_NextRow;
           Owned<CouchbaseDatasetBinder> m_oInputStream;
           TokenDeserializer m_tokenDeserializer;
           TokenSerializer m_tokenSerializer;
           unsigned m_nextParam;
           unsigned m_numParams;
           unsigned m_scriptFlags;
       };
} // couchbaseembed namespace
#endif
