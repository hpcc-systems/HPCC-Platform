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

#ifdef _WIN32
#define COUCHBASEEMBED_PLUGIN_CALL _cdecl
#ifdef COUCHBASEEMBED_PLUGIN_EXPORTS
#define COUCHBASEEMBED_PLUGIN_API __declspec(dllexport)
#else
#define COUCHBASEEMBED_PLUGIN_API __declspec(dllimport)
#endif
#else
#define COUCHBASEEMBED_PLUGIN_CALL
#define COUCHBASEEMBED_PLUGIN_API
#endif

//Using cpp wrapper from https://github.com/couchbaselabs/libcouchbase-cxx
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
#include "rtlfield_imp.hpp"
#include "roxiemem.hpp"


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
        typeError(expected, field ? field->name->queryStr() : nullptr);
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

    void handleDeserializeOutcome(DeserializationResult resultcode, const char * targetype, const char * culpritvalue)
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

    class CouchbaseRowStream : public RtlCInterface, implements IRowStream
    {
    public:
        CouchbaseRowStream(IEngineRowAllocator* _resultAllocator, Couchbase::Query * cbaseQuery);
        virtual ~CouchbaseRowStream();

        RTLIMPLEMENT_IINTERFACE
        virtual const void* nextRow();
        virtual void stop();
    private:
        Couchbase::Query *              m_CouchBaseQuery;   //!< pointer to couchbase query (holds results and metadata)

        Linked<IEngineRowAllocator>     m_resultAllocator;  //!< Pointer to allocator used when building result rows
        bool                            m_shouldRead;       //!< If true, we should continue trying to read more messages
        StringArray                     m_Rows;             //!< Local copy of result rows
        __int64                         m_currentRow;       //!< Current result row

    };

    class CouchbaseConnection : public CInterface
    {
    public:
        inline CouchbaseConnection(bool useSSL, const char * host, unsigned port, const char * bucketname, const char * user, const char * password, const char * connOptions)
        {
            m_connectionString.setf("couchbase%s://%s/%s%s", useSSL ? "s" : "", host, bucketname, connOptions);
            m_pCouchbaseClient = new Couchbase::Client(m_connectionString.str());//USER/PASS still needed
            m_pQuery = nullptr;
        }

        inline ~CouchbaseConnection()
        {
        }

        inline void connect()
        {
            m_connectionStatus = m_pCouchbaseClient->connect();
            if (!m_connectionStatus.success())
                failx("Failed to connect to couchbase instance: %s Reason: %s", m_connectionString.str(), m_connectionStatus.description());
        }

        Couchbase::Query * query(Couchbase::QueryCommand * qcommand);

    private:
        StringBuffer m_connectionString;
        Couchbase::Client * m_pCouchbaseClient;
        Couchbase::Status  m_connectionStatus;
        Couchbase::Query  * m_pQuery;

        CouchbaseConnection(const CouchbaseConnection &);
    };

    class CouchbaseRowBuilder : public CInterfaceOf<IFieldSource>
    {
    public:
        CouchbaseRowBuilder(IPropertyTree * resultrow) :  m_fieldsProcessedCount(0), m_rowFieldCount(0)
        {
            m_oResultRow.set(resultrow);
        }

        virtual bool getBooleanResult(const RtlFieldInfo *field)
        {
            const char * value = nextField(field);

            if (!value && !*value)
            {
                NullFieldProcessor p(field);
                return p.boolResult;
            }

            bool mybool;
            couchbaseembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mybool), "bool", value);
            return mybool;
        }

        virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
        {
            const char * value = nextField(field);

            if (!value || !*value)
            {
                NullFieldProcessor p(field);
                rtlStrToDataX(len, result, p.resultChars, p.stringResult);
                return;
            }
            rtlStrToDataX(len, result, strlen(value), value);   // This feels like it may not work to me - will preallocate rather larger than we want
        }

        virtual double getRealResult(const RtlFieldInfo *field)
        {
            const char * value = nextField(field);

            if (!value || !*value)
            {
                NullFieldProcessor p(field);
                return p.doubleResult;
            }

            double mydouble;
            couchbaseembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mydouble), "real", value);
            return mydouble;
        }

        virtual __int64 getSignedResult(const RtlFieldInfo *field)
        {
            const char * value = nextField(field);
            if (!value || !*value)
            {
                NullFieldProcessor p(field);
                return p.uintResult;
            }

            __int64 myint64;
            couchbaseembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myint64), "signed", value);
            return myint64;
        }

        virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
        {
            const char * value = nextField(field);
            if (!value || !*value)
            {
                NullFieldProcessor p(field);
                return p.uintResult;
            }

            unsigned __int64 myuint64;
            couchbaseembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myuint64), "unsigned", value);
            return myuint64;
        }

        virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
        {
             const char * value = nextField(field);

            if (!value || !*value)
            {
                NullFieldProcessor p(field);
                rtlStrToStrX(chars, result, p.resultChars, p.stringResult);
                return;
            }

            unsigned numchars = rtlUtf8Length(strlen(value), value);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
            rtlUtf8ToStrX(chars, result, numchars, value);
            return;
        }

        virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
        {
            getStringResult(field, chars, result);
            return;
        }

        virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
        {
            const char * value = nextField(field);

            if (!value || !*value)
            {
                NullFieldProcessor p(field);
                rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
                return;
            }

            unsigned numchars = rtlUtf8Length(strlen(value), value);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
            rtlUtf8ToUnicodeX(chars, result, numchars, value);
            return;
        }

        virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
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
            if (field)
            {
                RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *) field->type;
                value.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
            }
        }

        virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
        {
            UNSUPPORTED("Embedded Couchbase support error: processBeginSet() not supported");
        }

        virtual bool processNextSet(const RtlFieldInfo * field)
        {
            UNSUPPORTED("Embedded Couchbase support error: processNextSet() not supported");
            return false;
        }

        virtual void processBeginDataset(const RtlFieldInfo * field)
        {
            /*
             *
             *childRec := RECORD real x; real y; END;
             *parentRec := RECORD
             * childRec child1,                        <-- flatens out the childrec, this function would receive a field of name x
             * dataset(childRec) child2;               <-- keeps nested structure, this funciton would receive a field of name child2
             *END;
            */

            if (getNumFields(field->type->queryChildType()) > 0)
                m_oNestedField.set(m_oResultRow->queryBranch(field->name->queryStr()));
        }

        virtual void processBeginRow(const RtlFieldInfo * field)
        {
            m_fieldsProcessedCount = 0;
            m_rowFieldCount = getNumFields(field->type);
        }

        virtual bool processNextRow(const RtlFieldInfo * field)
        {
            return m_fieldsProcessedCount + 1 == m_rowFieldCount;;
        }

        virtual void processEndSet(const RtlFieldInfo * field)
        {
            UNSUPPORTED("Embedded Couchbase support error: processEndSet() not supported");
        }

        virtual void processEndDataset(const RtlFieldInfo * field)
        {
            if(m_oNestedField)
                m_oNestedField.clear();
        }

        virtual void processEndRow(const RtlFieldInfo * field)
        {
            if(m_oNestedField)
                m_oNestedField.clear();
        }

    protected:
        const char * nextField(const RtlFieldInfo * field)
        {
            m_fieldsProcessedCount++;
            if (!m_oResultRow)
                failx("Missing result row data");

            const char * fieldname = field->name->queryStr();
            if (!fieldname || !*fieldname)
                failx("Missing result column metadata (name)");

            if (!m_oResultRow->hasProp(fieldname))
            {
                VStringBuffer nxpath("locationData/%s", fieldname);
                if (m_oNestedField)
                {
                    if (!m_oNestedField->hasProp(fieldname))
                    {
                        StringBuffer xml;
                        toXML(m_oResultRow, xml);
                        failx("Result row does not contain field: %s: %s", fieldname, xml.str());
                    }

                    return m_oNestedField->queryProp(fieldname);
                }
            }
            return m_oResultRow->queryProp(fieldname);
        }
    private:
        TokenDeserializer m_tokenDeserializer;
        Owned<IPropertyTree> m_oResultRow;
        Owned<IPropertyTree> m_oNestedField;
        int m_fieldsProcessedCount;
        int m_rowFieldCount;;
    };
} // couchbaseembed namespace
#endif
