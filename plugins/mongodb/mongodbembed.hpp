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

#ifndef _MONGODBEMBED_INCL
#define _MONGODBEMBED_INCL

#ifdef MONGODBEMBED_PLUGIN_EXPORTS
#define MONGODBEMBED_PLUGIN_API DECL_EXPORT
#else
#define MONGODBEMBED_PLUGIN_API DECL_IMPORT
#endif

// Using cpp driver from http://mongocxx.org/mongocxx-v3/installation/linux/
#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/logger.hpp"
#include "mongocxx/pool.hpp"
#include "mongocxx/uri.hpp"
#include "mongocxx/exception/bulk_write_exception.hpp"
#include "mongocxx/exception/error_code.hpp"
#include "mongocxx/exception/logic_error.hpp"
#include "mongocxx/exception/operation_exception.hpp"
#include "mongocxx/exception/server_error_code.hpp"

#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"
#include "tokenserialization.hpp"
#include "rtlfield.hpp"
#include "roxiemem.hpp"

namespace mongodbembed
{
    extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
    extern void failx(const char *msg, ...) __attribute__((noreturn))  __attribute__((format(printf, 1, 2)));
    extern void fail(const char *msg) __attribute__((noreturn));

    static void typeError(const char *expected, const char * fieldname)
    {
        VStringBuffer msg("MongoDBembed: type mismatch - %s expected", expected);
        if (!isEmptyString(fieldname))
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

    enum PathNodeType {CPNTScalar, CPNTDataset, CPNTSet};

    struct PathTracker
    {
        StringBuffer nodeName;
        PathNodeType nodeType;
        unsigned int currentChildIndex;
        unsigned int childCount;
        unsigned int childrenProcessed;

        // Constructor given node name and dataset bool
        PathTracker(const StringBuffer& _nodeName, PathNodeType _nodeType)
            : nodeName(_nodeName), nodeType(_nodeType), currentChildIndex(0), childCount(0), childrenProcessed(0)
        {}

        // Copy constructor
        PathTracker(const PathTracker& other)
            : nodeName(other.nodeName), nodeType(other.nodeType), currentChildIndex(other.currentChildIndex), childCount(other.childCount), childrenProcessed(other.childrenProcessed)
        {}
    };

    /**
     * @brief Holds information about where to send the query, how to query, and the results of the query.
     * 
     */
    class MongoDBQuery
    {
    public:
        /**
         * @brief Stores the databaseName and CollectionName for executing the operations.
         * 
         * @param database MongoDB database to connect to.
         * @param collection MongoDB collection to connect to.
         * @param _connectionString Connection string for creating the mongocxx::uri.
         * @param _batchSize The number of documents MongoDB should return per batch.
         */
        MongoDBQuery(const char *database, const char *collection, const char *_connectionString, std::int32_t _batchSize) 
            : databaseName(database), collectionName(collection), connectionString(_connectionString), batchSize(_batchSize)
        {}

        /**
         * @brief Set the Embed object and remove leading characters.
         * 
         * @param eScript pointer to beginning of the script.
         */
        void setEmbed(const char *eScript) 
        {
            const char *script = eScript;
            const char *end;

            // Leading whitespace, new line characters, and tabs are removed.
            while(*script && (*script == ' ' || *script == '\n' || *script == '\t'))
                script++;

            end = script;
            while(*end && *end != '(') end++; // Get pointer to end of the command
            if(*end != '(')
                failx("Syntax Error: missing opening parenthesis");
            queryCMD = std::string(script, end - script); // Get a string of the query command

            embeddedScript = end + 1; // Save embedded script as what is after the parenthesis
            if(embeddedScript.find(";") == std::string::npos) failx("Syntax Error: missing semicolon");
            cursor = embeddedScript.c_str();
        }

        /**
         * @brief Gets pointer to database name.
         * 
         * @return const char* Name of the database.
         */
        const char* database()
        {
            return databaseName.c_str();
        }

        /**
         * @brief Gets pointer to collection name.
         * 
         * @return const char* Name of the collection.
         */
        const char* collection() 
        {
            return collectionName.c_str();
        }

        /**
         * @brief Gets pointer to script.
         * 
         * @return const char* Beginning of the script.
         */
        const char*& script() 
        {
            return cursor;
        }

        /**
         * @brief Gets pointer to command.
         * 
         * @return const char* Beginning of the command.
         */
        const char* cmd() 
        {
            return queryCMD.c_str();
        }
        
        /**
         * @brief Returns a pointer to the basic builder used for binding ECL datasets and rows.
         * 
         * @return bsoncxx::builder::basic::document* Pointer to builder.
         */
        bsoncxx::builder::basic::document* build() 
        {
            return &builder;
        } 

        /**
         * @brief Returns a pointer to the result rows from the MongoDB operation.
         * 
         * @return StringArray* Result rows in standard json.
         */
        StringArray* result() 
        {
            return &result_rows;
        }

        /**
         * @brief Returns a copy of the batch size to use in mongocxx::options
         * 
         * @return std::int32_t Batch Size
         */
        std::int32_t size()
        {
            return batchSize;
        }

        /**
         * @brief Returns a const char pointer to the connection string for
         * creating the uri.
         * 
         * @return The connection string as a const char pointer.
         */
        const char * uri()
        {
            return connectionString.str();
        }

    protected:
        std::string databaseName;                      //! Local copy of database name.
        std::string collectionName;                    //! Local copy of collection name.
        std::string embeddedScript;                    //! Local copy of the embedded script.
        std::string queryCMD;                          //! Local copy of the name of the MongoDB query operation.
        bsoncxx::builder::basic::document builder{};   //! Local copy of the basic builder used for bind ECL rows to documents.
        const char* cursor = nullptr;                  //! Pointer for keeping track of parsing the embedded script.
        StringArray result_rows;                       //! Local copy of result rows.
        std::int32_t batchSize;                        //! Batch Size for result rows.
        StringBuffer connectionString;                 //! Pointer to connection string for hashing and creating the uri.
    };

    /**
     * @brief Builds ECL Records from MongoDB result rows.
     * 
     */
    class MongoDBRowStream : public RtlCInterface, implements IRowStream
    {
    public:
        MongoDBRowStream(IEngineRowAllocator* _resultAllocator, std::shared_ptr<MongoDBQuery> _query);
        virtual ~MongoDBRowStream();

        RTLIMPLEMENT_IINTERFACE
        virtual const void* nextRow();
        virtual void stop();
    private:
        Linked<IEngineRowAllocator> m_resultAllocator;      //!< Pointer to allocator used when building result rows.
        bool m_shouldRead;                                  //!< If true, we should continue trying to read more messages.
        __int64 m_currentRow;                               //!< Current result row.
        std::shared_ptr<MongoDBQuery> m_query;              //!< Pointer to MongoDBQuery object.
    };

    /**
     * @brief Class for keeping a heap allocated MongoDB instance. The reason this is important is because
     * MongoDB only allows one instance to be created at any one time, so we create an instance on the heap
     * for every thread to share.
     * 
     */
    class MongoDBConnection 
    {
    private:
        typedef std::map<hash64_t, std::shared_ptr<mongocxx::client>> ObjMap;
        
    public:
        /**
         * @brief Creates a static reference to a MongoDB instance that is alive
         * for the entire time MongoDBEmbedFunctionContext is used.
         * 
         * @return MongoDBConnection& A reference to a MongoDBConnection
         * instance used for connecting to a database.
         */
        static MongoDBConnection& instance() 
        {
            static MongoDBConnection instance;
            return instance;
        }

        /**
         * @brief Configures the MongoDB instance for the client objects to use for connections. It 
         * should only be called once because only one instance object is allowed per program.
         * 
         * @param instance The instance object that is to be kept alive for multiple
         * threads to have access to.
         */
        void configure(std::unique_ptr<mongocxx::instance> && instance) 
        {
            _instance = std::move(instance);
        }

        /**
         * @brief Creates a client object using the specified connections string. The client is
         * added to the map of active connections.
         * 
         * @param connectionString The connection string for constructing the client object.
         */
        void create_connection(const char *connectionString) 
        {
            auto client_ptr = std::make_shared<mongocxx::client>(mongocxx::client{mongocxx::uri{connectionString}});

            // Use a hash of the connection string as the key to finding
            // any connection objects
            hash64_t key = rtlHash64VStr(connectionString, 0); 

            clientConnections[key] = client_ptr;
        }

        /**
         * @brief Acquires a mongocxx client from the connections map.
         * 
         * @param connectionString A string holding the connection parameters.
         * 
         * @return A shared pointer to the mongocxx:client object for connecting to the database.
         */
        std::shared_ptr<mongocxx::client> get_connection(const char *connectionString)
        {
            // Get key for client object
            hash64_t key = rtlHash64VStr(connectionString, 0);

            return clientConnections[key];
        }

    private:
        MongoDBConnection() = default;

        std::unique_ptr<mongocxx::instance> _instance = nullptr;        //!< Unique pointer to the single mongocxx::instance for the program.
        ObjMap clientConnections;                                       //!< Hashmap of MongoDB client connections.
    };

    /**
     * @brief Builds ECL records for MongoDBRowStream.
     * 
     */
    class MongoDBRowBuilder : public CInterfaceOf<IFieldSource>
    {
    public:
        MongoDBRowBuilder(IPropertyTree * resultrow)
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

    /**
     * @brief Binds ECL records to bson Documents
     * 
     */
    class MongoDBRecordBinder : public CInterfaceOf<IFieldProcessor>
    {
    public:
        MongoDBRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, std::shared_ptr<MongoDBQuery> _query, int _firstParam)
         : logctx(_logctx), typeInfo(_typeInfo), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam) 
        {
            query = _query;
        }

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

        const RtlTypeInfo *typeInfo = nullptr;
        const IContextLogger &logctx;
        std::shared_ptr<MongoDBQuery> query = nullptr;
        int firstParam;
        RtlFieldStrInfo dummyField;
        int thisParam;
        TokenSerializer m_tokenSerializer;
    };

    /**
     * @brief Binds an ECL dataset to a vector of bson documents.
     * 
     */
    class MongoDBDatasetBinder : public MongoDBRecordBinder
    {
    public:
        /**
         * @brief Construct a new MongoDBDataset Binder object
         * 
         * @param _logctx logger for building the dataset.
         * @param _input Stream of input of dataset.
         * @param _typeInfo Field type info.
         * @param _query Holds the builder object for creating the documents.
         * @param _firstParam Index of the first param.
         */
        MongoDBDatasetBinder(const IContextLogger &_logctx, IRowStream * _input, const RtlTypeInfo *_typeInfo, std::shared_ptr<MongoDBQuery> _query, int _firstParam)
          : input(_input), MongoDBRecordBinder(_logctx, _typeInfo, _query, _firstParam)
        {
        }

        /**
         * @brief Gets the next ECL row and binds it to a MongoDB document.
         * 
         * @return true If there is a row to process.
         * @return false If there are no rows left.
         */
        bool bindNext()
        {
            roxiemem::OwnedConstRoxieRow nextRow = (const byte *) input->ungroupedNextRow();
            if (!nextRow)
                return false;
            processRow((const byte *) nextRow.get());   // Bind the variables for the current row
            return true;
        }

        /**
         * @brief Binds all the rows of the dataset to bson documents and adds them to an array for calling insert many.
         * 
         * @param m_oMDBConnection Connection object for getting acces to the mongocxx::instance object.
         */
        void executeAll(MongoDBConnection * m_oMDBConnection)
        {
            auto cmd = std::string(query->cmd());

            auto conn = m_oMDBConnection->instance().get_connection(query->uri());
            mongocxx::database db = (*conn)[query->database()];
            mongocxx::collection coll = db[query->collection()];

            std::vector<bsoncxx::document::value> documents;
            bsoncxx::stdx::optional<mongocxx::result::insert_many> result;
            // Binds a single row from the dataset at a time
            while (bindNext())
            {
                // Gets the view of the ECL row
                bsoncxx::document::value value = query->build()->extract();
                documents.push_back(value);
                query->build()->clear();  // Clear the document for the next row to be processed
            }
            if(cmd != "insert") failx("Only insert operations are supported for Dataset arguments.");

            try 
            {
                result = coll.insert_many(documents);
            } 
            catch (const mongocxx::exception &e) 
            {
                failx("insert_many execution error: %s", e.what());
            }

            if(result)
            {
                StringBuffer json;
                json.appendf("{ \"inserted_count\" : %i }", result->inserted_count());
                query->result()->append(json.str());
            }
        }

    protected:
        Owned<IRowStream> input;
    };
    
    /**
     * @brief Main interface for the engine to interact with the plugin. The get functions return results to the engine and the Rowstream and 
     * 
     */
    class MongoDBEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
    {
       public:
           MongoDBEmbedFunctionContext(const IContextLogger &_logctx, const char *options, unsigned _flags);
           virtual ~MongoDBEmbedFunctionContext();
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

           void runQuery(mongocxx::database &db, mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value);
           void mdbinsert(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value);
           void mdbfind(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value, std::string com);
           void mdbaggregate(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value);
           void mdbrunCommand(mongocxx::database &db, bsoncxx::document::view &view, bsoncxx::document::value &doc_value);
           void mdbcreateIndex(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value);
           void mdbdelete(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value, std::string com);
           void mdbupdate(mongocxx::collection &coll, bsoncxx::document::view &view, bsoncxx::document::value &doc_value, std::string com);
           void buildPipeline(const bsoncxx::document::view &view, mongocxx::pipeline &stages, const char *&start);
           template<typename T>
           void buildDocument(T &builder, const bsoncxx::document::view &view, const char *&start);
           template<typename T>
           void bindKVP(T &builder, const bsoncxx::document::view &view, const char *&start);
           template<typename T>
           void buildArray(T &ctx, const bsoncxx::document::view &view, const char *&start);

       protected:
           void execute();
           unsigned countBindings(const char *query);
           const char * findUnquoted(const char *query, char searchFor);
           unsigned checkNextParam(const char *name);
           const IContextLogger &logctx;
           Owned<IPropertyTreeIterator> m_resultrow;

           int m_NextRow;                                   //! Next Row to process.
           Owned<MongoDBDatasetBinder> m_oInputStream;      //! Input Stream used for building a dataset.
           MongoDBConnection * m_oMDBConnection;            //! Pointer to a heap allocated mongocxx::instance.
           std::shared_ptr<MongoDBQuery> query;             //! Holds the script for performing query ,and the database and collection to operate on.
           
           TokenDeserializer m_tokenDeserializer;
           TokenSerializer m_tokenSerializer;
           unsigned m_nextParam;                            //! Index of the next parameter to process.
           unsigned m_numParams;                            //! Number of parameters in the function definition.
           unsigned m_scriptFlags;                          //! Count of flags raised by embedded script.
       };
} // mongodbembed namespace
#endif
