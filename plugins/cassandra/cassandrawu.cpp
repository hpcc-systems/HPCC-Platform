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
#include "cassandra.h"
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
#include "jsort.hpp"
#include "jptree.hpp"
#include "jregexp.hpp"
#include "dadfs.hpp"

#include "workunit.hpp"
#include "workunit.ipp"

#include "cassandraembed.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace cassandraembed {

#define ATTRIBUTES_NAME "attributes"

void addElement(IPTree *parent, const char *name, const CassValue *value)
{
    switch (cass_value_type(value))
    {
    case CASS_VALUE_TYPE_UNKNOWN:
        // It's a NULL - ignore it (or we could add empty element...)
        break;

    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR:
    {
        rtlDataAttr str;
        unsigned chars;
        getUTF8Result(NULL, value, chars, str.refstr());
        StringAttr s(str.getstr(), rtlUtf8Size(chars, str.getstr()));
        parent->addProp(name, s);
        break;
    }

    case CASS_VALUE_TYPE_INT:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_VARINT:
        parent->addPropInt64(name, getSignedResult(NULL, value));
        break;

    case CASS_VALUE_TYPE_BLOB:
    {
        rtlDataAttr data;
        unsigned bytes;
        getDataResult(NULL, value, bytes, data.refdata());
        parent->addPropBin(name, bytes, data.getbytes());
        break;
    }
    case CASS_VALUE_TYPE_BOOLEAN:
        parent->addPropBool(name, getBooleanResult(NULL, value));
        break;

    case CASS_VALUE_TYPE_DOUBLE:
    case CASS_VALUE_TYPE_FLOAT:
    {
        double v = getRealResult(NULL, value);
        StringBuffer s;
        s.append(v);
        parent->addProp(name, s);
        break;
    }
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_SET:
    {
        CassandraIterator elems(cass_iterator_from_collection(value));
        Owned<IPTree> list = createPTree(name);
        while (cass_iterator_next(elems))
            addElement(list, "item", cass_iterator_get_value(elems));
        parent->addPropTree(name, list.getClear());
        break;
    }
    case CASS_VALUE_TYPE_MAP:
    {
        CassandraIterator elems(cass_iterator_from_map(value));
        if (strcmp(name, ATTRIBUTES_NAME)==0 && isString(cass_value_primary_sub_type(value)))
        {
            while (cass_iterator_next(elems))
            {
                rtlDataAttr str;
                unsigned chars;
                getStringResult(NULL, cass_iterator_get_map_key(elems), chars, str.refstr());
                StringBuffer s("@");
                s.append(chars, str.getstr());
                addElement(parent, s, cass_iterator_get_map_value(elems));
            }
        }
        else
        {
            Owned<IPTree> map = createPTree(name);
            while (cass_iterator_next(elems))
            {
                if (isString(cass_value_primary_sub_type(value)))
                {
                    rtlDataAttr str;
                    unsigned chars;
                    getStringResult(NULL, cass_iterator_get_map_key(elems), chars, str.refstr());
                    StringAttr s(str.getstr(), chars);
                    addElement(map, s, cass_iterator_get_map_value(elems));
                }
                else
                {
                    Owned<IPTree> mapping = createPTree("mapping");
                    addElement(mapping, "key", cass_iterator_get_map_key(elems));
                    addElement(mapping, "value", cass_iterator_get_map_value(elems));
                    map->addPropTree("mapping", mapping.getClear());
                }
            }
            parent->addPropTree(name, map.getClear());
        }
        break;
    }
    default:
        DBGLOG("Column type %d not supported", cass_value_type(value));
        UNSUPPORTED("Column type");
    }
}

void bindElement(CassStatement *statement, IPTree *parent, unsigned idx, const char *name, CassValueType type)
{
    if (parent->hasProp(name) || strcmp(name, ATTRIBUTES_NAME)==0)
    {
        switch (type)
        {
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR:
        {
            const char *value = parent->queryProp(name);
            if (value)
                check(cass_statement_bind_string(statement, idx, value));
            break;
        }

        case CASS_VALUE_TYPE_INT:
            check(cass_statement_bind_int32(statement, idx, parent->getPropInt(name)));
            break;
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_VARINT:
            check(cass_statement_bind_int64(statement, idx, parent->getPropInt64(name)));
            break;

        case CASS_VALUE_TYPE_BLOB:
        {
            MemoryBuffer buf;
            parent->getPropBin(name, buf);
            check(cass_statement_bind_bytes(statement, idx, (const cass_byte_t*)buf.toByteArray(), buf.length()));
            break;
        }
        case CASS_VALUE_TYPE_BOOLEAN:
            check(cass_statement_bind_bool(statement, idx, (cass_bool_t) parent->getPropBool(name)));
            break;

        case CASS_VALUE_TYPE_DOUBLE:
            check(cass_statement_bind_double(statement, idx, atof(parent->queryProp(name))));
            break;
        case CASS_VALUE_TYPE_FLOAT:
            check(cass_statement_bind_float(statement, idx, atof(parent->queryProp(name))));
            break;
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            Owned<IPTree> child = parent->getPropTree(name);
            unsigned numItems = child->getCount("item");
            if (numItems)
            {
                CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_SET, numItems));
                Owned<IPTreeIterator> items = child->getElements("item");
                ForEach(*items)
                {
                    // We don't know the subtypes - we can assert that we only support string, for most purposes, I suspect
                    if (strcmp(name, "list1")==0)
                        check(cass_collection_append_int32(collection, items->query().getPropInt(NULL)));
                    else
                        check(cass_collection_append_string(collection, items->query().queryProp(NULL)));
                }
                check(cass_statement_bind_collection(statement, idx, collection));
            }
            break;
        }

        case CASS_VALUE_TYPE_MAP:
        {
            // We don't know the subtypes - we can assert that we only support string, for most purposes, I suspect
            if (strcmp(name, ATTRIBUTES_NAME)==0)
            {
                Owned<IAttributeIterator> attrs = parent->getAttributes();
                unsigned numItems = attrs->count();
                ForEach(*attrs)
                {
                    numItems++;
                }
                if (numItems)
                {
                    CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                    ForEach(*attrs)
                    {
                        const char *key = attrs->queryName();
                        const char *value = attrs->queryValue();
                        check(cass_collection_append_string(collection, key+1));  // skip the @
                        check(cass_collection_append_string(collection, value));
                    }
                    check(cass_statement_bind_collection(statement, idx, collection));
                }
            }
            else
            {
                Owned<IPTree> child = parent->getPropTree(name);
                unsigned numItems = child->numChildren();
                // MORE - if the cassandra driver objects to there being fewer than numItems supplied, we may need to recode using a second pass.
                if (numItems)
                {
                    CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                    Owned<IPTreeIterator> items = child->getElements("*");
                    ForEach(*items)
                    {
                        IPTree &item = items->query();
                        const char *key = item.queryName();
                        const char *value = item.queryProp(NULL);
                        if (key && value)
                        {
                            check(cass_collection_append_string(collection, key));
                            check(cass_collection_append_string(collection, value));
                        }
                    }
                    check(cass_statement_bind_collection(statement, idx, collection));
                }
            }
            break;
        }
        default:
            DBGLOG("Column type %d not supported", type);
            UNSUPPORTED("Column type");
        }
    }
}


extern void cassandraToGenericXML()
{
    CassandraCluster cluster(cass_cluster_new());
    cass_cluster_set_contact_points(cluster, "127.0.0.1");

    CassandraSession session(cass_session_new());
    CassandraFuture future(cass_session_connect_keyspace(session, cluster, "test"));
    future.wait("connect");
    CassandraStatement statement(cass_statement_new("select * from tbl1 where name = 'name1';", 0));
    CassandraFuture future2(cass_session_execute(session, statement));
    future2.wait("execute");
    CassandraResult result(cass_future_get_result(future2));
    StringArray names;
    UnsignedArray types;
    for (int i = 0; i < cass_result_column_count(result); i++)
    {
        const char *column;
        size_t length;
        cass_result_column_name(result, i, &column, &length);
        StringBuffer name(length, column);
        names.append(name);
        types.append(cass_result_column_type(result, i));
    }
    // Now fetch the rows
    Owned<IPTree> xml = createPTree("tbl1");
    CassandraIterator rows(cass_iterator_from_result(result));
    while (cass_iterator_next(rows))
    {
        CassandraIterator cols(cass_iterator_from_row(cass_iterator_get_row(rows)));
        Owned<IPTree> row = createPTree("row");
        unsigned colidx = 0;
        while (cass_iterator_next(cols))
        {
            const CassValue *value = cass_iterator_get_column(cols);
            const char *name = names.item(colidx);
            addElement(row, name, value);
            colidx++;
        }
        xml->addPropTree("row", row.getClear());
    }
    xml->setProp("row[1]/name", "newname");
    StringBuffer buf;
    toXML(xml, buf);
    DBGLOG("%s", buf.str());

    // Now try going the other way...
    // For this we need to know the expected names (can fetch them from system table) and types (ditto, potentially, though a dummy select may be easier)
    StringBuffer colNames;
    StringBuffer values;
    ForEachItemIn(idx, names)
    {
        colNames.append(",").append(names.item(idx));
        values.append(",?");
    }
    VStringBuffer insertQuery("INSERT into tbl1 (%s) values (%s);", colNames.str()+1, values.str()+1);
    Owned<IPTreeIterator> xmlRows = xml->getElements("row");
    ForEach(*xmlRows)
    {
        IPropertyTree *xmlrow = &xmlRows->query();
        CassandraStatement update(cass_statement_new(insertQuery.str(), names.length()));
        ForEachItemIn(idx, names)
        {
            bindElement(update, xmlrow, idx, names.item(idx), (CassValueType) types.item(idx));
        }
        // MORE - use a batch
        CassandraFuture future3(cass_session_execute(session, update));
        future2.wait("insert");
    }

}

//--------------------------------------------

#define CASS_WU_QUERY_EXPIRES  (1000*60*5)
#define CASS_WORKUNIT_POSTSORT_LIMIT 10000
#define CASS_SEARCH_PREFIX_SIZE 2
#define NUM_PARTITIONS 2

static const CassValue *getSingleResult(const CassResult *result)
{
    const CassRow *row = cass_result_first_row(result);
    if (row)
        return cass_row_get_column(row, 0);
    else
        return NULL;
}

static StringBuffer &getCassString(StringBuffer &str, const CassValue *value)
{
    const char *output;
    size_t length;
    check(cass_value_get_string(value, &output, &length));
    return str.append(length, output);
}

struct CassandraColumnMapper
{
    virtual ~CassandraColumnMapper() {}
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value) = 0;
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal) = 0;
};

static class StringColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        rtlDataAttr str;
        unsigned chars;
        getUTF8Result(NULL, value, chars, str.refstr());
        StringAttr s(str.getstr(), rtlUtf8Size(chars, str.getstr()));
        row->setProp(name, s);
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        const char *value = row->queryProp(name);
        if (!value)
            return false;
        if (statement)
            statement->bindString(idx, value);
        return true;
    }
} stringColumnMapper;

static class RequiredStringColumnMapper : public StringColumnMapper
{
public:
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        const char *value = row->queryProp(name);
        if (!value)
            value = "";
        if (statement)
            statement->bindString(idx, value);
        return true;
    }
} requiredStringColumnMapper;

static class SuppliedStringColumnMapper : public StringColumnMapper
{
public:
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *, const char *userVal)
    {
        if (statement)
            statement->bindString(idx, userVal);
        return true;
    }
} suppliedStringColumnMapper;

static class BlobColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        rtlDataAttr str;
        unsigned chars;
        getDataResult(NULL, value, chars, str.refdata());
        row->setPropBin(name, chars, str.getbytes());
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        MemoryBuffer value;
        row->getPropBin(name, value);
        if (value.length())
        {
            if (statement)
                check(cass_statement_bind_bytes(*statement, idx, (const cass_byte_t *) value.toByteArray(), value.length()));
            return true;
        }
        else
            return false;
    }
} blobColumnMapper;

static class TimeStampColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        // never fetched (that may change?)
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        // never bound, but does need to be included in the ?
        return true;
    }
} timestampColumnMapper;

static class HashRootNameColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        throwUnexpected(); // we never return the partition column
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        if (statement)
        {
            int hash = rtlHash32VStr(row->queryName(), 0) % NUM_PARTITIONS;
            check(cass_statement_bind_int32(*statement, idx, hash));
        }
        return true;
    }
} hashRootNameColumnMapper;

static class RootNameColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        rtlDataAttr str;
        unsigned chars;
        getUTF8Result(NULL, value, chars, str.refstr());
        StringAttr s(str.getstr(), rtlUtf8Size(chars, str.getstr()));
        row->renameProp("/", s);
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        if (statement)
        {
            const char *value = row->queryName();
            statement->bindString(idx, value);
        }
        return true;
    }
} rootNameColumnMapper;

// WuidColumnMapper is used for columns containing a wuid that is NOT in the resulting XML - it
// is an error to try to map such a column to/from the XML representation

static class WuidColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        throwUnexpected();
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        throwUnexpected();
    }
} wuidColumnMapper;

static class GraphIdColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        rtlDataAttr str;
        unsigned chars;
        getUTF8Result(NULL, value, chars, str.refstr());
        StringAttr s(str.getstr(), rtlUtf8Size(chars, str.getstr()));
        if (strcmp(s, "Running")==0)  // The input XML structure is a little odd
            return row;
        else
        {
            if (!row->hasProp(s))
                row->addPropTree(s, createPTree());
            return row->queryPropTree(s);
        }
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        const char *value = row->queryName();
        if (!value)
            return false;
        if (statement)
            statement->bindString(idx, value);
        return true;
    }
} graphIdColumnMapper;

static class ProgressColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        rtlDataAttr str;
        unsigned chars;
        getDataResult(NULL, value, chars, str.refdata());  // Stored as a blob in case we want to compress
        IPTree *child = createPTreeFromXMLString(chars, str.getstr());  // For now, assume we did not compress!
        row->addPropTree(child->queryName(), child);
        return child;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        // MORE - may need to read, and probably should write, compressed.
        StringBuffer value;
        ::toXML(row, value, 0, 0);
        if (value.length())
        {
            if (statement)
                check(cass_statement_bind_bytes(*statement, idx, (const cass_byte_t *) value.str(), value.length()));
            return true;
        }
        else
            return false;
    }
} progressColumnMapper;

static class BoolColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        row->addPropBool(name, getBooleanResult(NULL, value));
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * userVal)
    {
        if (row->hasProp(name))
        {
            if (statement)
            {
                bool value = row->getPropBool(name, false);
                check(cass_statement_bind_bool(*statement, idx, value ? cass_true : cass_false));
            }
            return true;
        }
        else
            return false;
    }
} boolColumnMapper;

static class PrefixSearchColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *, const char *userVal)
    {
        return _fromXML(statement, idx, row, userVal, CASS_SEARCH_PREFIX_SIZE, true);
    }
protected:
    static bool _fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *xpath, unsigned prefixLength, bool uc)
    {
        const char *columnVal = row->queryProp(xpath);
        if (columnVal)
        {
            if (statement)
            {
                StringBuffer buf(columnVal);
                if (uc)
                    buf.toUpperCase();
                if (prefixLength && prefixLength < buf.length())
                    statement->bindString_n(idx, buf, prefixLength);
                else
                    statement->bindString(idx, buf);
            }
            return true;
        }
        else
            return false;
    }
} prefixSearchColumnMapper;

static class SearchColumnMapper : public PrefixSearchColumnMapper
{
public:
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *, const char *userVal)
    {
        return _fromXML(statement, idx, row, userVal, 0, true);
    }
} searchColumnMapper;

static class LCSearchColumnMapper : public PrefixSearchColumnMapper
{
public:
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *, const char *userVal)
    {
        return _fromXML(statement, idx, row, userVal, 0, false);
    }
} lcSearchColumnMapper;

static class IntColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        if (name)
            row->addPropInt(name, getSignedResult(NULL, value));
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        if (row->hasProp(name))
        {
            if (statement)
            {
                int value = row->getPropInt(name);
                check(cass_statement_bind_int32(*statement, idx, value));
            }
            return true;
        }
        else
            return false;
    }
} intColumnMapper;

static class DefaultedIntColumnMapper : public IntColumnMapper
{
public:
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char * defaultValue)
    {
        if (statement)
        {
            int value = row->getPropInt(name, atoi(defaultValue));
            check(cass_statement_bind_int32(*statement, idx, value));
        }
        return true;
    }
} defaultedIntColumnMapper;

static class BigIntColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        row->addPropInt64(name, getSignedResult(NULL, value));
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        if (row->hasProp(name))
        {
            if (statement)
            {
                __int64 value = row->getPropInt64(name);
                check(cass_statement_bind_int64(*statement, idx, value));
            }
            return true;
        }
        else
            return false;
    }
} bigintColumnMapper;

static class SubgraphIdColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        __int64 id = getSignedResult(NULL, value);
        if (id)
            row->addPropInt64(name, id);
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        if (statement)
        {
            int value = row->getPropInt(name);
            check(cass_statement_bind_int64(*statement, idx, value));
        }
        return true;
    }
} subgraphIdColumnMapper;

static class SimpleMapColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        Owned<IPTree> map = createPTree(name);
        CassandraIterator elems(cass_iterator_from_map(value));
        while (cass_iterator_next(elems))
        {
            rtlDataAttr str;
            unsigned chars;
            getStringResult(NULL, cass_iterator_get_map_key(elems), chars, str.refstr());
            StringAttr s(str.getstr(), chars);
            stringColumnMapper.toXML(map, s, cass_iterator_get_map_value(elems));
        }
        row->addPropTree(name, map.getClear());
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        Owned<IPTree> child = row->getPropTree(name);
        if (child)
        {
            unsigned numItems = child->numChildren();
            if (numItems)
            {
                if (statement)
                {
                    CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                    Owned<IPTreeIterator> items = child->getElements("*");
                    ForEach(*items)
                    {
                        IPTree &item = items->query();
                        const char *key = item.queryName();
                        const char *value = item.queryProp(NULL);
                        if (key && value)
                        {
                            check(cass_collection_append_string(collection, key));
                            check(cass_collection_append_string(collection, value));
                        }
                    }
                    check(cass_statement_bind_collection(*statement, idx, collection));
                }
                return true;
            }
        }
        return false;
    }
} simpleMapColumnMapper;

static class AttributeMapColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        CassandraIterator elems(cass_iterator_from_map(value));
        while (cass_iterator_next(elems))
        {
            rtlDataAttr str;
            unsigned chars;
            getStringResult(NULL, cass_iterator_get_map_key(elems), chars, str.refstr());
            StringBuffer s("@");
            s.append(chars, str.getstr());
            stringColumnMapper.toXML(row, s, cass_iterator_get_map_value(elems));
        }
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        // NOTE - name here provides a list of attributes that we should NOT be mapping
        Owned<IAttributeIterator> attrs = row->getAttributes();
        unsigned numItems = 0;
        ForEach(*attrs)
        {
            StringBuffer key = attrs->queryName();
            key.append('@');
            if (strstr(name, key) == NULL)
                numItems++;
        }
        if (numItems)
        {
            if (statement)
            {
                CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                ForEach(*attrs)
                {
                    StringBuffer key = attrs->queryName();
                    key.append('@');
                    if (strstr(name, key) == NULL)
                    {
                        const char *value = attrs->queryValue();
                        check(cass_collection_append_string(collection, attrs->queryName()+1));  // skip the @
                        check(cass_collection_append_string(collection, value));
                    }
                }
                check(cass_statement_bind_collection(*statement, idx, collection));
            }
            return true;
        }
        else
            return false;
    }
} attributeMapColumnMapper;

static class ElementMapColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        CassandraIterator elems(cass_iterator_from_map(value));
        while (cass_iterator_next(elems))
        {
            rtlDataAttr str;
            unsigned chars;
            getStringResult(NULL, cass_iterator_get_map_key(elems), chars, str.refstr());
            StringBuffer elemName(chars, str.getstr());
            stringColumnMapper.toXML(row, elemName, cass_iterator_get_map_value(elems));
        }
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        // NOTE - name here provides a list of elements that we should NOT be mapping
        Owned<IPTreeIterator> elems = row->getElements("*");
        unsigned numItems = 0;
        ForEach(*elems)
        {
            IPTree &item = elems->query();
            StringBuffer key('@');
            key.append(item.queryName());
            key.append('@');
            if (strstr(name, key) == NULL)
            {
                const char *value = item.queryProp(".");
                if (value)
                    numItems++;
            }
        }
        if (numItems)
        {
            if (statement)
            {
                CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                ForEach(*elems)
                {
                    IPTree &item = elems->query();
                    StringBuffer key('@');
                    key.append(item.queryName());
                    key.append('@');
                    if (strstr(name, key) == NULL)
                    {
                        const char *value = item.queryProp(".");
                        if (value)
                        {
                            check(cass_collection_append_string(collection, item.queryName()));
                            check(cass_collection_append_string(collection, value));
                        }
                    }
                }
                check(cass_statement_bind_collection(*statement, idx, collection));
            }
            return true;
        }
        else
            return false;
    }
} elementMapColumnMapper;

static class SubtreeMapColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        CassandraIterator elems(cass_iterator_from_map(value));
        while (cass_iterator_next(elems))
        {
            rtlDataAttr str;
            unsigned chars;
            getStringResult(NULL, cass_iterator_get_map_key(elems), chars, str.refstr());
            StringBuffer elemName(chars, str.getstr());
            const CassValue *value = cass_iterator_get_map_value(elems);
            StringBuffer valStr;
            getCassString(valStr, value);
            if (valStr.length() && valStr.charAt(0)== '<')
                row->setPropTree(elemName, createPTreeFromXMLString(valStr));
        }
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        // NOTE - name here provides a list of elements that we SHOULD be mapping
        Owned<IPTreeIterator> elems = row->getElements("*");
        unsigned numItems = 0;
        ForEach(*elems)
        {
            IPTree &item = elems->query();
            StringBuffer key("@");
            key.append(item.queryName());
            key.append('@');
            if (strstr(name, key) != NULL)
            {
                if (item.numChildren())
                    numItems++;
            }
        }
        if (numItems)
        {
            if (statement)
            {
                CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                ForEach(*elems)
                {
                    IPTree &item = elems->query();
                    StringBuffer key("@");
                    key.append(item.queryName());
                    key.append('@');
                    if (strstr(name, key) != NULL)
                    {
                        if (item.numChildren())
                        {
                            StringBuffer x;
                            ::toXML(&item, x);
                            check(cass_collection_append_string(collection, item.queryName()));
                            check(cass_collection_append_string(collection, x));
                        }
                    }
                }
                check(cass_statement_bind_collection(*statement, idx, collection));
            }
            return true;
        }
        else
            return false;
    }
} subTreeMapColumnMapper;

static class QueryTextColumnMapper : public StringColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        // Name is "Query/Text ...
        IPTree *query = row->queryPropTree("Query");
        if (!query)
        {
            query = createPTree("Query");
            query = row->setPropTree("Query", query);
            row->setProp("Query/@fetchEntire", "1"); // Compatibility...
        }
        return StringColumnMapper::toXML(query, "Text", value);
    }
} queryTextColumnMapper;

static class GraphMapColumnMapper : implements CassandraColumnMapper
{
public:
    GraphMapColumnMapper(const char *_elemName, const char *_nameAttr)
    : elemName(_elemName), nameAttr(_nameAttr)
    {
    }
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        Owned<IPTree> map = createPTree(name);
        CassandraIterator elems(cass_iterator_from_map(value));
        while (cass_iterator_next(elems))
        {
            rtlDataAttr str;
            unsigned chars;
            getStringResult(NULL, cass_iterator_get_map_value(elems), chars, str.refstr());
            Owned<IPTree> child = createPTreeFromXMLString(chars, str.getstr());
            map->addPropTree(elemName, child.getClear());
        }
        row->addPropTree(name, map.getClear());
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        Owned<IPTree> child = row->getPropTree(name);
        if (child)
        {
            unsigned numItems = child->numChildren();
            if (numItems)
            {
                if (statement)
                {
                    CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, numItems));
                    Owned<IPTreeIterator> items = child->getElements("*");
                    ForEach(*items)
                    {
                        IPTree &item = items->query();
                        const char *key = item.queryProp(nameAttr);
                        // MORE - may need to read, and probably should write, compressed. At least for graphs
                        StringBuffer value;
                        ::toXML(&item, value, 0, 0);
                        if (key && value.length())
                        {
                            check(cass_collection_append_string(collection, key));
                            check(cass_collection_append_string(collection, value));
                        }
                    }
                    check(cass_statement_bind_collection(*statement, idx, collection));
                }
                return true;
            }
        }
        return false;
    }
private:
    const char *elemName;
    const char *nameAttr;
} graphMapColumnMapper("Graph", "@name"), workflowMapColumnMapper("Item", "@wfid");

static class AssociationsMapColumnMapper : public GraphMapColumnMapper
{
public:
    AssociationsMapColumnMapper(const char *_elemName, const char *_nameAttr)
    : GraphMapColumnMapper(_elemName, _nameAttr)
    {
    }
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        // Name is "Query/Associated ...
        IPTree *query = row->queryPropTree("Query");
        if (!query)
        {
            query = createPTree("Query");
            row->setPropTree("Query", query);
            row->setProp("Query/@fetchEntire", "1"); // Compatibility...
        }
        return GraphMapColumnMapper::toXML(query, "Associated", value);
    }
} associationsMapColumnMapper("File", "@filename");

static class WarningsMapColumnMapper : implements CassandraColumnMapper
{
public:
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        CassandraIterator elems(cass_iterator_from_map(value));
        while (cass_iterator_next(elems))
        {
            unsigned code = getUnsignedResult(NULL, cass_iterator_get_map_key(elems));
            VStringBuffer xpath("OnWarnings/OnWarning[@code='%u']", code);
            IPropertyTree * mapping = row->queryPropTree(xpath);
            if (!mapping)
            {
                IPropertyTree * onWarnings = ensurePTree(row, "OnWarnings");
                mapping = onWarnings->addPropTree("OnWarning", createPTree());
                mapping->setPropInt("@code", code);
            }
            rtlDataAttr str;
            unsigned chars;
            getStringResult(NULL, cass_iterator_get_map_value(elems), chars, str.refstr());
            StringBuffer s(chars, str.getstr());
            mapping->setProp("@severity", s);
        }
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        if (!row->hasProp("OnWarnings/OnWarning"))
            return false;
        else
        {
            if (statement)
            {
                CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, 5));
                Owned<IPTreeIterator> elems = row->getElements("OnWarnings/OnWarning");
                ForEach(*elems)
                {
                    IPTree &item = elems->query();
                    unsigned code = item.getPropInt("@code", 0);
                    const char *value = item.queryProp("@severity");
                    if (value)
                    {
                        check(cass_collection_append_int32(collection, code));
                        check(cass_collection_append_string(collection, value));
                    }
                }
                check(cass_statement_bind_collection(*statement, idx, collection));
            }
            return true;
        }
    }
} warningsMapColumnMapper;

static class PluginListColumnMapper : implements CassandraColumnMapper
{
public:
    PluginListColumnMapper(const char *_elemName, const char *_nameAttr)
    : elemName(_elemName), nameAttr(_nameAttr)
    {
    }
    virtual IPTree *toXML(IPTree *row, const char *name, const CassValue *value)
    {
        Owned<IPTree> map = name ? createPTree(name) : LINK(row);
        CassandraIterator elems(cass_iterator_from_collection(value));
        while (cass_iterator_next(elems))
        {
            Owned<IPTree> child = createPTree(elemName);
            stringColumnMapper.toXML(child, nameAttr, cass_iterator_get_value(elems));
            map->addPropTree(elemName, child.getClear());
        }
        if (name)
            row->addPropTree(name, map.getClear());
        return row;
    }
    virtual bool fromXML(CassandraStatement *statement, unsigned idx, IPTree *row, const char *name, const char *userVal)
    {
        Owned<IPTree> child = row->getPropTree(name);
        if (child)
        {
            unsigned numItems = child->numChildren();
            if (numItems)
            {
                if (statement)
                {
                    CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_LIST, numItems));
                    Owned<IPTreeIterator> items = child->getElements("*");
                    ForEach(*items)
                    {
                        IPTree &item = items->query();
                        const char *value = item.queryProp(nameAttr);
                        if (value)
                            check(cass_collection_append_string(collection, value));
                    }
                    check(cass_statement_bind_collection(*statement, idx, collection));
                }
                return true;
            }
        }
        return false;
    }
private:
    const char *elemName;
    const char *nameAttr;
} pluginListColumnMapper("Plugin", "@dllname"), subfileListColumnMapper("Subfile", "@name");

struct CassandraXmlMapping
{
    const char *columnName;
    const char *columnType;
    const char *xpath;
    CassandraColumnMapper &mapper;
};

struct CassandraTableInfo
{
    const char *x;
    const CassandraXmlMapping *mappings;
};

static const CassandraXmlMapping workunitsMappings [] =
{
    {"partition", "int", NULL, hashRootNameColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"clustername", "text", "@clusterName", stringColumnMapper},
    {"jobname", "text", "@jobName", stringColumnMapper},
    {"priorityclass", "text", "@priorityClass", stringColumnMapper},
    {"wuScope", "text", "@scope", stringColumnMapper},
    {"submitID", "text", "@submitID", stringColumnMapper},
    {"state", "text", "@state", stringColumnMapper},

    {"action", "text", "Action", stringColumnMapper},
    {"protected", "boolean", "@protected", boolColumnMapper},
    {"scheduled", "text", "@timeScheduled", stringColumnMapper},   // Should store as a date?
    {"totalThorTime", "text", "@totalThorTime", stringColumnMapper},  // We store in the wu ptree as a collatable string. Need to force to one partition too
    {"appvalues", "map<text, text>", "@Application@", subTreeMapColumnMapper}, // MORE - change to a custom map to make searchable

    {"debug", "map<text, text>", "Debug", simpleMapColumnMapper},
    {"attributes", "map<text, text>", "@wuid@clusterName@jobName@priorityClass@protected@scope@submitID@state@timeScheduled@totalThorTime@", attributeMapColumnMapper},  // name is the suppression list, note trailing @
    {"graphs", "map<text, text>", "Graphs", graphMapColumnMapper}, // MORE - make me lazy...
    {"plugins", "list<text>", "Plugins", pluginListColumnMapper},
    {"query", "text", "Query/Text", queryTextColumnMapper},        // MORE - make me lazy...
    {"associations", "map<text, text>", "Query/Associated", associationsMapColumnMapper},
    {"workflow", "map<text, text>", "Workflow", workflowMapColumnMapper},
    {"onWarnings", "map<int, text>", "OnWarnings/OnWarning", warningsMapColumnMapper},

    // These are catchalls for anything not processed above or in a child table

    {"elements", "map<text, text>", "@Action@Application@Debug@Exceptions@FilesRead@Graphs@Results@Statistics@Plugins@Query@Variables@Temporaries@Workflow@", elementMapColumnMapper},  // name is the suppression list, note trailing @
    {"subtrees", "map<text, text>", "@Process@Tracing@", subTreeMapColumnMapper},  // name is the INCLUSION list, note trailing @

    { NULL, "workunits", "((partition), wuid)|CLUSTERING ORDER BY (wuid DESC)", stringColumnMapper}
};

static const CassandraXmlMapping workunitInfoMappings [] =  // A cut down version of the workunit mappings - used when querying with no key
{
    {"partition", "int", NULL, hashRootNameColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"clustername", "text", "@clusterName", stringColumnMapper},
    {"jobname", "text", "@jobName", stringColumnMapper},
    {"priorityclass", "text", "@priorityClass", stringColumnMapper},
    {"wuScope", "text", "@scope", stringColumnMapper},
    {"submitID", "text", "@submitID", stringColumnMapper},
    {"state", "text", "@state", stringColumnMapper},

    {"action", "text", "Action", stringColumnMapper},
    {"protected", "boolean", "@protected", boolColumnMapper},
    {"scheduled", "text", "@timeScheduled", stringColumnMapper},   // Should store as a date?
    {"totalThorTime", "text", "@totalThorTime", stringColumnMapper},  // We store in the wu ptree as a collatable string. Need to force to one partition too
    {"appvalues", "map<text, text>", "@Application@", subTreeMapColumnMapper},
    { NULL, "workunits", "((partition), wuid)|CLUSTERING ORDER BY (wuid DESC)", stringColumnMapper}
};

// The following describes the search table - this contains copies of the basic wu information but keyed by different fields

static const CassandraXmlMapping searchMappings [] =
{
    {"xpath", "text", NULL, suppliedStringColumnMapper},
    {"fieldPrefix", "text", NULL, prefixSearchColumnMapper},
    {"fieldValue", "text", NULL, searchColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"clustername", "text", "@clusterName", stringColumnMapper},
    {"jobname", "text", "@jobName", stringColumnMapper},
    {"priorityclass", "text", "@priorityClass", stringColumnMapper},
    {"scope", "text", "@scope", stringColumnMapper},
    {"submitID", "text", "@submitID", stringColumnMapper},
    {"state", "text", "@state", stringColumnMapper},

    {"action", "text", "Action", stringColumnMapper},
    {"protected", "boolean", "@protected", boolColumnMapper},
    {"scheduled", "text", "@timeScheduled", stringColumnMapper},   // Should store as a date?
    {"totalThorTime", "text", "@totalThorTime", stringColumnMapper},  // We store in the wu ptree as a collatable string. Need to force to one partition too
    {"appvalues", "map<text, text>", "@Application@", subTreeMapColumnMapper},
    { NULL, "workunitsSearch", "((xpath, fieldPrefix), fieldValue, wuid)|CLUSTERING ORDER BY (fieldValue ASC, wuid DESC)", stringColumnMapper}
};

// The fields we can search by. These are a subset of the fields in the basic workunit info that is returned from a search. A row is created in the search table for each of these, for each workunit.

const char * searchPaths[] = { "@submitID", "@clusterName", "@jobName", "@priorityClass", "@protected", "@scope", "@state", "@totalThorTime", NULL};

static const CassandraXmlMapping uniqueSearchMappings [] =
{
    {"xpath", "text", NULL, suppliedStringColumnMapper},
    {"fieldPrefix", "text", NULL, prefixSearchColumnMapper},  // Leading N chars, upper-cased
    {"fieldValue", "text", NULL, searchColumnMapper},    // upper-cased
    {"origFieldValue", "text", NULL, lcSearchColumnMapper},  // original case
    { NULL, "uniqueSearchValues", "((xpath, fieldPrefix), fieldValue, origFieldValue)|CLUSTERING ORDER BY (fieldValue ASC)", stringColumnMapper}
};

// The fields we can wild search by. We store these in the uniqueSearchMappings table so we can translate wildcards into sets
// We also add application name/key combinations, but we have to special-case that

const char * wildSearchPaths[] = { "@submitID", "@clusterName", "@jobName", NULL};

static const CassandraXmlMapping filesReadSearchMappings [] =
{
    {"name", "text", "@name", stringColumnMapper},
    {"wuid", "text", NULL, suppliedStringColumnMapper},
    { NULL, "filesReadSearchValues", "((name), wuid)|CLUSTERING ORDER BY (wuid DESC)", stringColumnMapper}
};

/*
 * Some thoughts on the secondary tables:
 * 1. To support (trailing) wildcards we will need to split the key into two - the leading N chars and the rest. Exactly what N is will depend on the installation size.
 *    Too large and users will complain, but too small would hinder partitioning of the values across Cassandra nodes. 1 or 2 may be enough.
 * 2. I could combine all the secondary tables into 1 with a field indicating the type of the key. The key field would be repeated though... Would it help?
 *    I'm not sure it really changes a lot - adds a bit of noise into the partitioner...
 *    Actually, it does mean that the updates and deletes can all be done with a single Cassandra query, though whether that has any advantages over multiple in a batch I don't know
 *    It MAY well make it easier to make sure that searches are case-insensitive, since we'll generally need to separate out the search field from the display field to achieve that
 * 3. Sort orders are tricky - I can use the secondary table to deliver sorted by one field as long as it is the one I am filtering by (but if is is I probably don't need it sorted!
 *
 */

// The following describe child tables - all keyed by wuid

enum ChildTablesEnum { WuExceptionsChild, WuStatisticsChild, WuGraphProgressChild, WuResultsChild, WuVariablesChild, WuTemporariesChild, WuFilesReadChild,ChildTablesSize };

struct ChildTableInfo
{
    const char *parentElement;
    const char *childElement;
    ChildTablesEnum index;
    const CassandraXmlMapping *mappings;
};

static const CassandraXmlMapping wuExceptionsMappings [] =
{
    {"partition", "int", NULL, hashRootNameColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"sequence", "int", "@sequence", intColumnMapper},
    {"attributes", "map<text, text>", "", attributeMapColumnMapper},
    {"value", "text", ".", stringColumnMapper},
    { NULL, "wuExceptions", "((partition, wuid), sequence)", stringColumnMapper}
};

static const ChildTableInfo wuExceptionsTable =
{
    "Exceptions", "Exception",
    WuExceptionsChild,
    wuExceptionsMappings
};

static const CassandraXmlMapping wuStatisticsMappings [] =
{
    {"partition", "int", NULL, hashRootNameColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"ts", "bigint", "@ts", bigintColumnMapper},  // MORE - should change this to a timeuuid ?
    {"kind", "text", "@kind", stringColumnMapper},
    {"creator", "text", "@creator", stringColumnMapper},
    {"scope", "text", "@scope", stringColumnMapper},
    {"attributes", "map<text, text>", "@ts@kind@creator@scope@", attributeMapColumnMapper},
    { NULL, "wuStatistics", "((partition, wuid), ts, kind, creator, scope)", stringColumnMapper}
};

static const ChildTableInfo wuStatisticsTable =
{
    "Statistics", "Statistic",
    WuStatisticsChild,
    wuStatisticsMappings
};

static const CassandraXmlMapping wuGraphProgressMappings [] =
{
    {"partition", "int", NULL, hashRootNameColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"graphID", "text", NULL, graphIdColumnMapper},
    {"progress", "blob", NULL, progressColumnMapper},  // NOTE - order of these is significant - this creates the subtree that ones below will modify
    {"subgraphID", "text", "@id", subgraphIdColumnMapper},
    {"state", "int", "@_state", intColumnMapper},
    { NULL, "wuGraphProgress", "((partition, wuid), graphid, subgraphid)", stringColumnMapper}
};

static const ChildTableInfo wuGraphProgressTable =
{
    "Bit of a", "Special case",
    WuGraphProgressChild,
    wuGraphProgressMappings
};

#define resultTableFields \
        {"partition", "int", NULL, hashRootNameColumnMapper},  \
        {"wuid", "text", NULL, rootNameColumnMapper},          \
        {"sequence", "int", "@sequence", defaultedIntColumnMapper},     \
        {"name", "text", "@name", stringColumnMapper},         \
        {"format", "text", "@format", stringColumnMapper},     /* xml, xmlset, csv, or null to mean raw. Could probably switch to int if we wanted, or drop altogether since included in attributes? */ \
        {"status", "text", "@status", stringColumnMapper},     \
        {"attributes", "map<text, text>", "@sequence@name@format@status@", attributeMapColumnMapper},  /* name is the suppression list. We could consider folding format/status into this? */ \
        {"rowcount", "int", "rowCount", intColumnMapper},      /* This is the number of rows in result (which may be stored in a file rather than in value) */ \
        {"totalrowcount", "bigint", "totalRowCount", bigintColumnMapper},  /* This is the number of rows in value */ \
        {"schemaRaw", "blob", "SchemaRaw", blobColumnMapper},              \
        {"logicalName", "text", "logicalName", stringColumnMapper},        /* either this or value will be present once result status is "calculated" */ \
        {"value", "blob", "Value", blobColumnMapper}

static const CassandraXmlMapping wuResultsMappings [] =
{
    resultTableFields,
    { NULL, "wuResults", "((partition, wuid), sequence)", stringColumnMapper}
};

static const ChildTableInfo wuResultsTable =
{
    "Results", "Result",
    WuResultsChild,
    wuResultsMappings
};

// This looks very similar to the above, but the key is different...

static const CassandraXmlMapping wuVariablesMappings [] =
{
    resultTableFields,
    { NULL, "wuVariables", "((partition, wuid), sequence, name)", stringColumnMapper}
};

static const ChildTableInfo wuVariablesTable =
{
    "Variables", "Variable",
    WuVariablesChild,
    wuVariablesMappings
};

// Again, very similar, but mapped to a different area of the XML

static const CassandraXmlMapping wuTemporariesMappings [] =
{
    resultTableFields,
    { NULL, "wuTemporaries", "((partition, wuid), sequence, name)", stringColumnMapper}
};

static const ChildTableInfo wuTemporariesTable =
{
    "Temporaries", "Variable",
    WuTemporariesChild,
    wuTemporariesMappings
};

static const CassandraXmlMapping wuFilesReadMappings [] =
{
    {"partition", "int", NULL, hashRootNameColumnMapper},
    {"wuid", "text", NULL, rootNameColumnMapper},
    {"name", "text", "@name", stringColumnMapper},
    {"cluster", "text", "@cluster", stringColumnMapper},
    {"useCount", "int", "@useCount", intColumnMapper}, // MORE - could think about using a counter column, but would mess up the commit paradigm
    {"subfiles", "list<text>", NULL, subfileListColumnMapper},
    { NULL, "wuFilesRead", "((partition, wuid), name)", stringColumnMapper}
};

static const ChildTableInfo wuFilesReadTable =
{
    "FilesRead", "File",
    WuFilesReadChild,
    wuFilesReadMappings
};

// Order should match the enum above
static const ChildTableInfo * const childTables [] = { &wuExceptionsTable, &wuStatisticsTable, &wuGraphProgressTable, &wuResultsTable, &wuVariablesTable, &wuTemporariesTable, &wuFilesReadTable, NULL };

interface ICassandraSession : public IInterface  // MORE - rename!
{
    virtual CassSession *querySession() const = 0;
    virtual CassandraPrepared *prepareStatement(const char *query) const = 0;
    virtual unsigned queryTraceLevel() const = 0;

    virtual const CassResult *fetchDataForWuid(const CassandraXmlMapping *mappings, const char *wuid, bool includeWuid) const = 0;
    virtual void deleteChildByWuid(const CassandraXmlMapping *mappings, const char *wuid, CassBatch *batch) const = 0;
};

void getBoundFieldNames(const CassandraXmlMapping *mappings, StringBuffer &names, StringBuffer &bindings, IPTree *inXML, const char *userVal, StringBuffer &tableName)
{
    while (mappings->columnName)
    {
        if (!inXML || mappings->mapper.fromXML(NULL, 0, inXML, mappings->xpath, userVal))
        {
            names.appendf(",%s", mappings->columnName);
            if (strcmp(mappings->columnType, "timeuuid")==0)
                bindings.appendf(",now()");
            else
                bindings.appendf(",?");
        }
        mappings++;
    }
    tableName.append(mappings->columnType);
}

void getFieldNames(const CassandraXmlMapping *mappings, StringBuffer &names, StringBuffer &tableName)
{
    while (mappings->columnName)
    {
        names.appendf(",%s", mappings->columnName);
        mappings++;
    }
    tableName.append(mappings->columnType);
}

const char *queryTableName(const CassandraXmlMapping *mappings)
{
    while (mappings->columnName)
        mappings++;
    return mappings->columnType;
}

StringBuffer & describeTable(const CassandraXmlMapping *mappings, StringBuffer &out)
{
    StringBuffer fields;
    while (mappings->columnName)
    {
        fields.appendf("%s %s,", mappings->columnName, mappings->columnType);
        mappings++;
    }
    StringArray options;
    options.appendList(mappings->xpath, "|");
    assertex(options.length()); // Primary key at least should be present!
    out.appendf("CREATE TABLE IF NOT EXISTS %s (%s PRIMARY KEY %s)", mappings->columnType, fields.str(), options.item(0));
    unsigned idx = 1;
    while (options.isItem(idx))
    {
        if (idx==1)
            out.append(" WITH ");
        else
            out.append(", ");
        out.append(options.item(idx));
        idx++;
    }
    out.append(';');
    return out;
}

const CassResult *executeQuery(CassSession *session, CassStatement *statement)
{
    CassandraFuture future(cass_session_execute(session, statement));
    future.wait("executeQuery");
    return cass_future_get_result(future);
}

void deleteSecondaryByKey(const char * xpath, const char *key, const char *wuid, const ICassandraSession *sessionCache, CassBatch *batch)
{
    if (key)
    {
        StringBuffer ucKey(key);
        ucKey.toUpperCase();
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(searchMappings, names, tableName);
        VStringBuffer deleteQuery("DELETE from %s where xpath=? and fieldPrefix=? and fieldValue=? and wuid=?;", tableName.str());
        Owned<CassandraPrepared> prepared = sessionCache->prepareStatement(deleteQuery);
        CassandraStatement update(cass_prepared_bind(*prepared));
        update.bindString(0, xpath);
        if (ucKey.length() < CASS_SEARCH_PREFIX_SIZE)
            update.bindString(1, ucKey);
        else
            update.bindString_n(1, ucKey, CASS_SEARCH_PREFIX_SIZE);
        update.bindString(2, ucKey);
        update.bindString(3, wuid);
        check(cass_batch_add_statement(batch, update));
    }
}

void executeSimpleCommand(CassSession *session, const char *command)
{
    CassandraStatement statement(cass_statement_new(command, 0));
    CassandraFuture future(cass_session_execute(session, statement));
    future.wait("execute");
}

void ensureTable(CassSession *session, const CassandraXmlMapping *mappings)
{
    StringBuffer schema;
    executeSimpleCommand(session, describeTable(mappings, schema));
}

extern void simpleXMLtoCassandra(const ICassandraSession *session, CassBatch *batch, const CassandraXmlMapping *mappings, IPTree *inXML, const char *userVal = NULL)
{
    StringBuffer names;
    StringBuffer bindings;
    StringBuffer tableName;
    getBoundFieldNames(mappings, names, bindings, inXML, userVal, tableName);
    VStringBuffer insertQuery("INSERT into %s (%s) values (%s);", tableName.str(), names.str()+1, bindings.str()+1);
    Owned<CassandraPrepared> prepared = session->prepareStatement(insertQuery);
    CassandraStatement update(cass_prepared_bind(*prepared));
    unsigned bindidx = 0;
    while (mappings->columnName)
    {
        if (mappings->mapper.fromXML(&update, bindidx, inXML, mappings->xpath, userVal))
            bindidx++;
        mappings++;
    }
    check(cass_batch_add_statement(batch, update));
}

extern void deleteSimpleXML(const ICassandraSession *session, CassBatch *batch, const CassandraXmlMapping *mappings, IPTree *inXML, const char *userVal = NULL)
{
    StringBuffer names;
    StringBuffer tableName;
    getFieldNames(mappings, names, tableName);
    VStringBuffer deleteQuery("DELETE from %s where name=? and wuid=?", tableName.str());
    Owned<CassandraPrepared> prepared = session->prepareStatement(deleteQuery);
    CassandraStatement update(cass_prepared_bind(*prepared));
    unsigned bindidx = 0;
    while (mappings->columnName)
    {
        if (mappings->mapper.fromXML(&update, bindidx, inXML, mappings->xpath, userVal))
            bindidx++;
        mappings++;
    }
    check(cass_batch_add_statement(batch, update));
}

extern void addUniqueValue(const ICassandraSession *session, CassBatch *batch, const char *xpath, const char *value)
{
    StringBuffer bindings;
    StringBuffer names;
    StringBuffer tableName;
    getBoundFieldNames(uniqueSearchMappings, names, bindings, NULL, NULL, tableName);
    VStringBuffer insertQuery("INSERT into %s (%s) values (%s);", tableName.str(), names.str()+1, bindings.str()+1);
    Owned<CassandraPrepared> prepared = session->prepareStatement(insertQuery);
    CassandraStatement update(cass_prepared_bind(*prepared));
    update.bindString(0, xpath);
    StringBuffer ucValue(value);
    ucValue.toUpperCase();
    if (ucValue.length() < CASS_SEARCH_PREFIX_SIZE)
        update.bindString(1, ucValue);
    else
        update.bindString_n(1, ucValue, CASS_SEARCH_PREFIX_SIZE);
    update.bindString(2, ucValue);
    update.bindString(3, value);
    check(cass_batch_add_statement(batch, update));
}

extern void childXMLRowtoCassandra(const ICassandraSession *session, CassBatch *batch, const CassandraXmlMapping *mappings, const char *wuid, IPTree &row, const char *userVal)
{
    StringBuffer bindings;
    StringBuffer names;
    StringBuffer tableName;
    getBoundFieldNames(mappings, names, bindings, &row, userVal, tableName);
    VStringBuffer insertQuery("INSERT into %s (%s) values (%s);", tableName.str(), names.str()+1, bindings.str()+1);
    Owned<CassandraPrepared> prepared = session->prepareStatement(insertQuery);
    CassandraStatement update(cass_prepared_bind(*prepared));
    check(cass_statement_bind_int32(update, 0, rtlHash32VStr(wuid, 0) % NUM_PARTITIONS));
    update.bindString(1, wuid);
    unsigned bindidx = 2; // We already bound wuid and partition
    unsigned colidx = 2; // We already bound wuid and partition
    while (mappings[colidx].columnName)
    {
        if (mappings[colidx].mapper.fromXML(&update, bindidx, &row, mappings[colidx].xpath, userVal))
            bindidx++;
        colidx++;
    }
    check(cass_batch_add_statement(batch, update));
}

extern void childXMLtoCassandra(const ICassandraSession *session, CassBatch *batch, const CassandraXmlMapping *mappings, const char *wuid, IPTreeIterator *elements, const char *userVal)
{
    if (elements->first())
    {
        do
        {
            childXMLRowtoCassandra(session, batch, mappings, wuid, elements->query(), userVal);
        }
        while (elements->next());
    }
}

extern void childXMLtoCassandra(const ICassandraSession *session, CassBatch *batch, const CassandraXmlMapping *mappings, IPTree *inXML, const char *xpath, const char *defaultValue)
{
    Owned<IPTreeIterator> elements = inXML->getElements(xpath);
    childXMLtoCassandra(session, batch, mappings, inXML->queryName(), elements, defaultValue);
}

/*
extern void graphProgressXMLtoCassandra(CassSession *session, IPTree *inXML)
{
    StringBuffer names;
    StringBuffer bindings;
    StringBuffer tableName;
    int numBound = getFieldNames(graphProgressMappings, names, bindings, tableName);
    VStringBuffer insertQuery("INSERT into %s (%s) values (%s);", tableName.str(), names.str()+1, bindings.str()+1);
    DBGLOG("%s", insertQuery.str());
    CassandraBatch batch(cass_batch_new(CASS_BATCH_TYPE_UNLOGGED));
    CassandraFuture futurePrep(cass_session_prepare(session, insertQuery));
    futurePrep.wait("prepare statement");
    CassandraPrepared prepared(cass_future_get_prepared(futurePrep));

    Owned<IPTreeIterator> graphs = inXML->getElements("./graph*");
    ForEach(*graphs)
    {
        IPTree &graph = graphs->query();
        Owned<IPTreeIterator> subgraphs = graph.getElements("./node");
        ForEach(*subgraphs)
        {
            IPTree &subgraph = subgraphs->query();
            CassandraStatement update(cass_prepared_bind(prepared));
            graphProgressMappings[0].mapper.fromXML(update, 0, inXML, graphProgressMappings[0].xpath);
            graphProgressMappings[1].mapper.fromXML(update, 1, &graph, graphProgressMappings[1].xpath);
            unsigned colidx = 2;
            while (graphProgressMappings[colidx].columnName)
            {
                graphProgressMappings[colidx].mapper.fromXML(update, colidx, &subgraph, graphProgressMappings[colidx].xpath);
                colidx++;
            }
            check(cass_batch_add_statement(batch, update));
        }
        // And one more with subgraphid = 0 for the graph status
        CassandraStatement update(cass_statement_new(insertQuery.str(), bindings.length()/2));
        graphProgressMappings[0].mapper.fromXML(update, 0, inXML, graphProgressMappings[0].xpath);
        graphProgressMappings[1].mapper.fromXML(update, 1, &graph, graphProgressMappings[1].xpath);
        check(cass_statement_bind_int64(update, 3, 0)); // subgraphId can't be null, as it's in the key
        unsigned colidx = 4;  // we skip progress and subgraphid
        while (graphProgressMappings[colidx].columnName)
        {
            graphProgressMappings[colidx].mapper.fromXML(update, colidx, &graph, graphProgressMappings[colidx].xpath);
            colidx++;
        }
        check(cass_batch_add_statement(batch, update));
    }
    if (inXML->hasProp("Running"))
    {
        IPTree *running = inXML->queryPropTree("Running");
        CassandraStatement update(cass_statement_new(insertQuery.str(), bindings.length()/2));
        graphProgressMappings[0].mapper.fromXML(update, 0, inXML, graphProgressMappings[0].xpath);
        graphProgressMappings[1].mapper.fromXML(update, 1, running, graphProgressMappings[1].xpath);
        graphProgressMappings[2].mapper.fromXML(update, 2, running, graphProgressMappings[2].xpath);
        check(cass_statement_bind_int64(update, 3, 0)); // subgraphId can't be null, as it's in the key
        check(cass_batch_add_statement(batch, update));
    }
    CassandraFuture futureBatch(cass_session_execute_batch(session, batch));
    futureBatch.wait("execute");
}

extern void cassandraToGraphProgressXML(CassSession *session, const char *wuid)
{
    CassandraResult result(fetchDataForWu(wuid, session, graphProgressMappings));
    Owned<IPTree> progress = createPTree(wuid);
    CassandraIterator rows(cass_iterator_from_result(result));
    while (cass_iterator_next(rows))
    {
        CassandraIterator cols(cass_iterator_from_row(cass_iterator_get_row(rows)));
        unsigned colidx = 1;  // wuid is not returned
        IPTree *ptree = progress;
        while (cass_iterator_next(cols))
        {
            assertex(graphProgressMappings[colidx].columnName);
            const CassValue *value = cass_iterator_get_column(cols);
            // NOTE - this relies on the fact that progress is NULL when subgraphId=0, so that the status and id fields
            // get set on the graph instead of on the child node in those cases.
            if (value && !cass_value_is_null(value))
                ptree = graphProgressMappings[colidx].mapper.toXML(ptree, graphProgressMappings[colidx].xpath, value);
            colidx++;
        }
    }
    StringBuffer out;
    toXML(progress, out, 0, XML_SortTags|XML_Format);
    printf("%s", out.str());
}
*/

/*
extern void cassandraTestGraphProgressXML()
{
    CassandraCluster cluster(cass_cluster_new());
    cass_cluster_set_contact_points(cluster, "127.0.0.1");
    CassandraSession session(cass_session_new());
    CassandraFuture future(cass_session_connect_keyspace(session, cluster, "hpcc"));
    future.wait("connect");

    ensureTable(session, graphProgressMappings);
    Owned<IPTree> inXML = createPTreeFromXMLFile("/data/rchapman/hpcc/testing/regress/ecl/a.xml");
    graphProgressXMLtoCassandra(session, inXML);
    const char *wuid = inXML->queryName();
    cassandraToGraphProgressXML(session, wuid);
}

*/

static IPTree *rowToPTree(const char *xpath, const char *key, const CassandraXmlMapping *mappings, const CassRow *row)
{
    CassandraIterator cols(cass_iterator_from_row(row));
    Owned<IPTree> xml = createPTree("row");  // May be overwritten below if wuid field is processed
    if (xpath && *xpath && key && *key)
        xml->setProp(xpath, key);
    while (cass_iterator_next(cols))
    {
        assertex(mappings->columnName);
        const CassValue *value = cass_iterator_get_column(cols);
        if (value && !cass_value_is_null(value))
            mappings->mapper.toXML(xml, mappings->xpath, value);
        mappings++;
    }
    return xml.getClear();
}

/*
 * PostFilter represents a filter to be applied to a ConstWorkUnitInfo tree representation prior to returning it from an iterator
 */

interface IPostFilter : public IInterface
{
    virtual bool matches(IPTree &p) const = 0;
    virtual const char *queryValue() const = 0;
    virtual const char *queryXPath() const = 0;
    virtual WUSortField queryField() const = 0;
};

class PostFilter : public CInterfaceOf<IPostFilter>
{
public:
    PostFilter(WUSortField _field, const char *_value, bool _wild)
      : field(_field), xpath(queryFilterXPath(_field)), wild(_wild)
    {
        setValue(_value);
    }
    virtual bool matches(IPTree &p) const
    {
        const char *val = p.queryProp(xpath);
        if (val)
            return wild ? WildMatch(val, pattern) : strieq(val, pattern);
        else
            return false;
    }
    virtual const char *queryValue() const
    {
        return value.str();
    }
    void setValue(const char *_value)
    {
        if (wild)
        {
            VStringBuffer filter("*%s*", _value);
            pattern.set(filter);
        }
        else
            pattern.set(_value);
        value.set(_value);
    }
    virtual const char *queryXPath() const
    {
        return xpath;
    }
    virtual WUSortField queryField() const
    {
        return field;
    }
private:
    const char *xpath;
    StringAttr pattern;
    StringAttr value;
    WUSortField field;
    bool wild;
};

class AppValuePostFilter : public CInterfaceOf<IPostFilter>
{
public:
    AppValuePostFilter(const char *_name, const char *_value, bool _wild) : wild(_wild)
    {
        xpath.appendf("Application/%s", _name);
        setValue(_value);
    }
    virtual bool matches(IPTree &p) const
    {
        const char *val = p.queryProp(xpath);
        if (val)
            return wild ? WildMatch(val, pattern) : strieq(val, pattern);
        else
            return false;
    }
    virtual const char *queryValue() const
    {
        return value.str();
    }
    void setValue(const char *_value)
    {
        if (wild)
        {
            VStringBuffer filter("*%s*", _value);
            pattern.set(filter);
        }
        else
            pattern.set(_value);
        value.set(_value);
    }
    virtual const char *queryXPath() const
    {
        return xpath;
    }
    virtual WUSortField queryField() const
    {
        return WUSFappvalue;
    }
private:
    StringBuffer xpath;
    StringAttr pattern;
    StringAttr value;
    bool wild;
};


class CassSortableIterator : public CassandraIterator
{
public:
    CassSortableIterator(CassIterator *_iterator, unsigned _idx, int _compareColumn, bool _descending)
    : CassandraIterator(_iterator), idx(_idx), compareColumn(_compareColumn), descending(_descending)
    {

    }
    const CassSortableIterator *nextRow()
    {
        if (iterator && cass_iterator_next(iterator))
        {
            if (compareColumn != -1)
            {
                const CassRow *row = cass_iterator_get_row(iterator);
                getCassString(value.clear(), cass_row_get_column(row, compareColumn));
            }
            return this;
        }
        else
            return NULL;
    }
    void stop()
    {
        value.clear();
        set(NULL);
    }
    int compare(const CassSortableIterator *to) const
    {
        if (compareColumn==-1)
            return idx - to->idx;  // concat mode
        int ret = strcmp(value, to->value); // Note - empty StringBuffer always returns ""
        return descending ? -ret : ret;
    }
private:
    StringBuffer value;
    unsigned idx;
    int compareColumn;
    bool descending;
};

interface IConstWorkUnitIteratorEx : public IConstWorkUnitIterator
{
    virtual bool hasPostFilters() const = 0;
    virtual bool isMerging() const = 0;
    virtual void notePosition() const = 0;
};

/*
 *
 * The cache entries serve two purposes:
 *
 * 1. They allow us to map row numbers to values for the end of each page returned, which can make forward paging efficient when not post-sorting
 * 2. They allow us to preserve post-sort results in order to avoid having to re-retrieve them.
 */

class CCassandraWuUQueryCacheEntry : public CInterfaceOf<IInterface>
{
public:
    CCassandraWuUQueryCacheEntry()
    {
        hint = get_cycles_now(); // MORE - should do better perhaps?
        lastAccess = msTick();
    }
    __int64 queryHint() const
    {
        return hint;
    }
    void noteWuid(const char *wuid, const char *fieldValue, unsigned row)
    {
        CriticalBlock b(crit);
        // NOTE - we store one set of row information per page retrieved - and we normally traverse the pages
        // in order so appending to the end is better than (for example) binchopping
        ForEachItemInRev(idx, rows)
        {
            unsigned foundRow = rows.item(idx);
            if (foundRow==row)
            {
                assert(streq(wuids.item(idx), wuid));
                assert(streq(fieldValues.item(idx), fieldValue));
                return;
            }
            if (foundRow < row)
                break;
        }
        rows.add(row, idx+1);
        wuids.add(wuid, idx+1);
        fieldValues.add(fieldValue, idx+1);
    }
    IConstWorkUnitIteratorEx *getResult() const
    {
        CriticalBlock b(crit);
        return result.getLink();
    }
    void setResult(IConstWorkUnitIteratorEx *_result)
    {
        CriticalBlock b(crit);
        result.set(_result);
    }
    unsigned lookupStartRow(StringBuffer &wuid, StringBuffer &fieldValue, unsigned startOffset) const
    {
        // See if we can provide a base wuid to search above/below
        CriticalBlock b(crit);
        ForEachItemInRev(idx, rows)
        {
            unsigned foundRow = rows.item(idx);
            if (foundRow <= startOffset)
            {
                wuid.set(wuids.item(idx));
                fieldValue.set(fieldValues.item(idx));
                return foundRow;
            }
        }
        return 0;
    }
    void touch()
    {
        lastAccess = msTick();
    }
    inline unsigned queryLastAccess() const
    {
        return lastAccess;
    }
private:
    mutable CriticalSection crit;  // It's POSSIBLE that we could get two queries in hitting the cache at the same time, I think...
    UnsignedArray rows;
    StringArray wuids;
    StringArray fieldValues;
    Owned<IConstWorkUnitIteratorEx> result;
    __uint64 hint;
    unsigned lastAccess;
};

class CassMultiIterator : public CInterface, implements IRowProvider, implements ICompare, implements IConstWorkUnitIteratorEx
{
public:
    IMPLEMENT_IINTERFACE;
    CassMultiIterator(CCassandraWuUQueryCacheEntry *_cache, unsigned _startRowNum, int _compareColumn, bool _descending)
    : cache(_cache)
    {
        compareColumn = _compareColumn;
        descending = _descending;
        startRowNum = _startRowNum;
    }
    void setStartOffset(unsigned start)
    {
        startRowNum = start; // we managed to do a seek forward via a filter
    }
    void setCompareColumn(int _compareColumn)
    {
        assert(!inputs.length());
        compareColumn = _compareColumn;
    }
    void addResult(CassandraResult &result)
    {
        results.append(result);
    }
    void addPostFilters(IArrayOf<IPostFilter> &filters, unsigned start)
    {
        unsigned len = filters.length();
        while (start<len)
            postFilters.append(OLINK(filters.item(start++)));
    }
    void addPostFilter(PostFilter &filter)
    {
        postFilters.append(filter);
    }
    virtual bool hasPostFilters() const
    {
        return postFilters.length() != 0;
    }
    virtual bool isMerging() const
    {
        return results.length() > 1;
    }
    virtual bool first()
    {
        inputs.kill();
        ForEachItemIn(idx, results)
        {
            inputs.append(*new CassSortableIterator(cass_iterator_from_result(results.item(idx)), idx, compareColumn, descending));
        }
        merger.setown(createRowStreamMerger(inputs.length(), *this, this, false));
        rowNum = startRowNum;
        return next();
    }
    virtual void notePosition() const
    {
        if (cache && current)
        {
            cache->noteWuid(current->queryWuid(), lastThorTime, rowNum);
        }
    }
    virtual bool next()
    {
        Owned<IConstWorkUnitInfo> last = current.getClear();
        loop
        {
            const CassandraIterator *nextSource = nextMergedSource();
            if (!nextSource)
            {
                if (cache && last)
                {
                    cache->noteWuid(last->queryWuid(), lastThorTime, rowNum);
                }
                return false;
            }
            Owned<IPTree> wuXML = rowToPTree(NULL, NULL, workunitInfoMappings+1, cass_iterator_get_row(*nextSource)); // NOTE - this is relying on search mappings and wuInfoMappings being the same
            bool postFiltered = false;
            ForEachItemIn(pfIdx, postFilters)
            {
                if (!postFilters.item(pfIdx).matches(*wuXML))
                {
                    postFiltered = true;
                    break;
                }
            }
            if (!postFiltered)
            {
                current.setown(createConstWorkUnitInfo(*wuXML));
                lastThorTime.set(wuXML->queryProp("@totalThorTime"));
                rowNum++;
                return true;
            }
        }
    }
    virtual bool isValid()
    {
        return current != NULL;
    }
    virtual IConstWorkUnitInfo & query()
    {
        assertex(current);
        return *current.get();
    }
    const CassandraIterator *nextMergedSource()
    {
        return (const CassSortableIterator *) merger->nextRow();
    }
protected:
    virtual void linkRow(const void *row) {  }
    virtual void releaseRow(const void *row) {  }
    virtual const void *nextRow(unsigned idx)
    {
        CassSortableIterator &it = inputs.item(idx);
        return it.nextRow(); // returns either a pointer to the iterator, or NULL
    }
    virtual void stop(unsigned idx)
    {
        inputs.item(idx).stop();
    }
    virtual int docompare(const void *a, const void *b) const
    {
        // a and b point to to CassSortableIterator objects
        const CassSortableIterator *aa = (const CassSortableIterator *) a;
        const CassSortableIterator *bb = (const CassSortableIterator *) b;
        return aa->compare(bb);
    }
private:
    IArrayOf<CassandraResult> results;
    IArrayOf<CassSortableIterator> inputs;
    Owned<IRowStream> merger; // NOTE - must be destroyed before inputs is destroyed
    IArrayOf<IPostFilter> postFilters;
    Owned<IConstWorkUnitInfo> current;
    Linked<CCassandraWuUQueryCacheEntry> cache;
    StringAttr lastThorTime;
    int compareColumn;
    unsigned startRowNum;
    unsigned rowNum;
    bool descending;
};

class CassPostSortIterator : public CInterfaceOf<IConstWorkUnitIteratorEx>, implements ICompare
{
public:
    CassPostSortIterator(IConstWorkUnitIterator * _input, unsigned _sortorder, unsigned _limit)
      : input(_input), sortorder(_sortorder), limit(_limit)
    {
        idx = 0;
    }
    virtual bool first()
    {
        if (input)
        {
            readFirst();
            input.clear();
        }
        idx = 0;
        return sorted.isItem(idx);
    }
    virtual bool next()
    {
        idx++;
        if (sorted.isItem(idx))
            return true;
        return false;
    }
    virtual void notePosition() const
    {
    }
    virtual bool isValid()
    {
        return sorted.isItem(idx);
    }
    virtual IConstWorkUnitInfo & query()
    {
        return sorted.item(idx);
    }
    virtual bool hasPostFilters() const
    {
        return false;  // they are done by my input. But we may want to rename this function to indicate "may return more than asked" in which case would be true
    }
    virtual bool isMerging() const
    {
        return false;
    }
private:
    void readFirst()
    {
        ForEach(*input)
        {
            sorted.append(OLINK(input->query()));
            if (sorted.length()>=limit)
                break;
        }
        qsortvec((void **)sorted.getArray(0), sorted.length(), *this);
    }

    virtual int docompare(const void *a, const void *b) const
    {
        // a and b point to to IConstWorkUnitInfo objects
        const IConstWorkUnitInfo *aa = (const IConstWorkUnitInfo *) a;
        const IConstWorkUnitInfo *bb = (const IConstWorkUnitInfo *) b;
        int diff;
        switch (sortorder & 0xff)
        {
        case WUSFuser:
            diff = stricmp(aa->queryUser(), bb->queryUser());
            break;
        case WUSFcluster:
            diff = stricmp(aa->queryClusterName(), bb->queryClusterName());
            break;
        case WUSFjob:
            diff = stricmp(aa->queryJobName(), bb->queryJobName());
            break;
        case WUSFstate:
            diff = stricmp(aa->queryStateDesc(), bb->queryStateDesc());
            break;
        case WUSFprotected:
            diff = (int) bb->isProtected() - (int) aa->isProtected();
            break;
        case WUSFtotalthortime:
            diff = (int) (bb->getTotalThorTime() - bb->getTotalThorTime());
            break;
        case WUSFwuid:
            diff = stricmp(aa->queryWuid(), bb->queryWuid());  // Should never happen, since we always fetch with a wuid sort
            break;
        default:
            throwUnexpected();
        }
        if (sortorder & WUSFreverse)
            return -diff;
        else
            return diff;
    }

    Owned<IConstWorkUnitIterator> input;
    IArrayOf<IConstWorkUnitInfo> sorted;
    unsigned sortorder;
    unsigned idx;
    unsigned limit;
};

class SubPageIterator : public CInterfaceOf<IConstWorkUnitIteratorEx>
{
public:
    SubPageIterator(IConstWorkUnitIteratorEx *_input, unsigned _startOffset, unsigned _pageSize)
    : input(_input), startOffset(_startOffset), pageSize(_pageSize), idx(0)
    {
    }
    virtual bool first()
    {
        idx = 0;

        // MORE - put a seek into the Ex interface
        if (input->first())
        {
            for (int i = 0; i < startOffset;i++)
            {
                if (!input->next())
                    return false;
            }
            return true;
        }
        else
            return false;
    }
    virtual bool next()
    {
        idx++;
        if (idx >= pageSize)
        {
            input->notePosition();
            return false;
        }
        return input->next();
    }
    virtual void notePosition() const
    {
        input->notePosition();
    }
    virtual bool isValid()
    {
        return idx < pageSize && input->isValid();
    }
    virtual IConstWorkUnitInfo & query()
    {
        return input->query();
    }
    virtual bool hasPostFilters() const
    {
        return false;
    }
    virtual bool isMerging() const
    {
        return false;
    }
private:
    Owned<IConstWorkUnitIteratorEx> input;
    unsigned startOffset;
    unsigned pageSize;
    unsigned idx;
};

class CassJoinIterator : public CInterface, implements IConstWorkUnitIteratorEx
{
public:
    IMPLEMENT_IINTERFACE;
    CassJoinIterator(unsigned _compareColumn, bool _descending)
    {
        compareColumn = _compareColumn;
        descending = _descending;
    }

    void addResult(CassandraResult &result)
    {
        results.append(result);
    }

    void addPostFilter(IPostFilter &post)
    {
        postFilters.append(post);
    }

    virtual bool first()
    {
        if (!results.length())
            return false;
        inputs.kill();
        ForEachItemIn(idx, results)
        {
            Owned <CassSortableIterator> input = new CassSortableIterator(cass_iterator_from_result(results.item(idx)), idx, compareColumn, descending);
            if (!input->nextRow())
                return false;
            inputs.append(*input.getClear());

        }
        return next();
    }
    virtual bool next()
    {
        current.clear();
        loop
        {
            unsigned idx = 0;
            unsigned target = 0;
            unsigned matches = 1;  // I always match myself!
            unsigned sources = inputs.length();
            if (!sources)
                return false;
            while (matches < sources)
            {
                idx++;
                if (idx==sources)
                    idx = 0;
                int diff;
                loop
                {
                    assert(idx != target);
                    diff = inputs.item(idx).compare(&inputs.item(target));
                    if (diff >= 0)
                        break;
                    if (!inputs.item(idx).nextRow())
                    {
                        inputs.kill(); // Once any reaches EOF, we are done
                        return false;
                    }
                }
                if (diff > 0)
                {
                    target = idx;
                    matches = 1;
                }
                else
                    matches++;
            }
            Owned<IPTree> wuXML = rowToPTree(NULL, NULL, workunitInfoMappings+1, cass_iterator_get_row(inputs.item(0)));
            bool postFiltered = false;
            ForEachItemIn(pfIdx, postFilters)
            {
                if (!postFilters.item(pfIdx).matches(*wuXML))
                {
                    postFiltered = true;
                    break;
                }
            }
            if (!postFiltered)
            {
                current.setown(createConstWorkUnitInfo(*wuXML));
                ForEachItemIn(idx2, inputs)
                {
                    if (!inputs.item(idx2).nextRow())
                    {
                        inputs.clear(); // Make sure next() fails next time it is called
                        break;
                    }
                }
                return true;
            }
        }
    }
    virtual bool isValid()
    {
        return current != NULL;
    }
    virtual IConstWorkUnitInfo & query()
    {
        assertex(current);
        return *current.get();
    }
private:
    IArrayOf<CassandraResult> results;
    IArrayOf<CassSortableIterator> inputs;
    IArrayOf<IPostFilter> postFilters;
    Owned<IConstWorkUnitInfo> current;
    unsigned compareColumn;
    bool descending;
};

class CCassandraWorkUnit : public CLocalWorkUnit
{
public:
    IMPLEMENT_IINTERFACE;
    CCassandraWorkUnit(ICassandraSession *_sessionCache, IPTree *wuXML, ISecManager *secmgr, ISecUser *secuser)
        : sessionCache(_sessionCache), CLocalWorkUnit(secmgr, secuser)
    {
        CLocalWorkUnit::loadPTree(wuXML);
        allDirty = false;   // Debatable... depends where the XML came from! If we read it from Cassandra. it's not. Otherwise, it is...
        memset(childLoaded, 0, sizeof(childLoaded));
        abortDirty = true;
        abortState = false;
    }
    ~CCassandraWorkUnit()
    {
    }

    virtual void forceReload()
    {
        printStackReport();
        UNIMPLEMENTED;
        abortDirty = true;
    }

    virtual void cleanupAndDelete(bool deldll, bool deleteOwned, const StringArray *deleteExclusions)
    {
        const char *wuid = queryWuid();
        CLocalWorkUnit::cleanupAndDelete(deldll, deleteOwned, deleteExclusions);
        if (!batch)
            batch.setown(new CassandraBatch(cass_batch_new(CASS_BATCH_TYPE_UNLOGGED)));
        deleteChildren(wuid);
        deleteSecondaries(wuid);
        Owned<CassandraPrepared> prepared = sessionCache->prepareStatement("DELETE from workunits where partition=? and wuid=?;");
        CassandraStatement update(cass_prepared_bind(*prepared));
        check(cass_statement_bind_int32(update, 0, rtlHash32VStr(wuid, 0) % NUM_PARTITIONS));
        update.bindString(1, wuid);
        check(cass_batch_add_statement(*batch, update));
        CassandraFuture futureBatch(cass_session_execute_batch(sessionCache->querySession(), *batch));
        futureBatch.wait("execute");
        batch.clear();
    }

    virtual void commit()
    {
        CLocalWorkUnit::commit();
        if (sessionCache->queryTraceLevel() >= 8)
        {
            StringBuffer s; toXML(p, s); DBGLOG("CCassandraWorkUnit::commit\n%s", s.str());
        }
        if (batch)
        {
            const char *wuid = queryWuid();
            if (prev) // Holds the values of the "basic" info at the last commit
                updateSecondaries(wuid);
            simpleXMLtoCassandra(sessionCache, *batch, workunitsMappings, p);  // This just does the parent row
            if (allDirty)
            {
                // MORE - this delete is technically correct, but if we assert that the only place that copyWorkUnit is used is to populate an
                // empty newly-created WU, it is unnecessary.
                //deleteChildren(wuid);

                childXMLtoCassandra(sessionCache, *batch, wuResultsMappings, p, "Results/Result", "0");
                childXMLtoCassandra(sessionCache, *batch, wuVariablesMappings, p, "Variables/Variable", "-1"); // ResultSequenceStored
                childXMLtoCassandra(sessionCache, *batch, wuTemporariesMappings, p, "Temporaries/Variable", "-3"); // ResultSequenceInternal // NOTE - lookups may also request ResultSequenceOnce
                childXMLtoCassandra(sessionCache, *batch, wuExceptionsMappings, p, "Exceptions/Exception", 0);
                childXMLtoCassandra(sessionCache, *batch, wuStatisticsMappings, p, "Statistics/Statistic", 0);
                childXMLtoCassandra(sessionCache, *batch, wuFilesReadMappings, p, "FilesRead/File", 0);
            }
            else
            {
                HashIterator iter(dirtyPaths);
                ForEach (iter)
                {
                    const char *path = (const char *) iter.query().getKey();
                    const CassandraXmlMapping *table = *dirtyPaths.mapToValue(&iter.query());
                    if (sessionCache->queryTraceLevel()>2)
                        DBGLOG("Updating dirty path %s", path);
                    if (*path == '*')
                    {
                        sessionCache->deleteChildByWuid(table, wuid, *batch);
                        childXMLtoCassandra(sessionCache, *batch, table, p, path+1, 0);
                    }
                    else
                    {
                        IPTree *dirty = p->queryPropTree(path);
                        if (dirty)
                            childXMLRowtoCassandra(sessionCache, *batch, table, wuid, *dirty, 0);
                        else if (sessionCache->queryTraceLevel())
                        {
                            StringBuffer xml;
                            toXML(p, xml);
                            DBGLOG("Missing dirty element %s in %s", path, xml.str());
                        }
                    }
                }
                ForEachItemIn(d, dirtyResults)
                {
                    IWUResult &result = dirtyResults.item(d);
                    switch (result.getResultSequence())
                    {
                    case ResultSequenceStored:
                        childXMLRowtoCassandra(sessionCache, *batch, wuVariablesMappings,  wuid, *result.queryPTree(), "-1");
                        break;
                    case ResultSequenceInternal:
                    case ResultSequenceOnce:
                        childXMLRowtoCassandra(sessionCache, *batch, wuTemporariesMappings,  wuid, *result.queryPTree(), "-3");
                        break;
                    default:
                        childXMLRowtoCassandra(sessionCache, *batch, wuResultsMappings, wuid, *result.queryPTree(), "0");
                        break;
                    }
                }
            }
            CassandraFuture futureBatch(cass_session_execute_batch(sessionCache->querySession(), *batch));
            futureBatch.wait("execute");
            batch.setown(new CassandraBatch(cass_batch_new(CASS_BATCH_TYPE_UNLOGGED))); // Commit leaves it locked...
            prev.clear();
            allDirty = false;
            dirtyPaths.kill();
        }
        else
            DBGLOG("No batch present??");
    }

    virtual void setUser(const char *user)
    {
        if (trackSecondaryChange(user, "@submitID"))
            CLocalWorkUnit::setUser(user);
    }
    virtual void setClusterName(const char *cluster)
    {
        if (trackSecondaryChange(cluster, "@clusterName"))
            CLocalWorkUnit::setClusterName(cluster);
    }
    virtual void setJobName(const char *jobname)
    {
        if (trackSecondaryChange(jobname, "@jobName"))
            CLocalWorkUnit::setJobName(jobname);
    }
    virtual void setState(WUState state)
    {
        if (trackSecondaryChange(getWorkunitStateStr(state), "@state"))
            CLocalWorkUnit::setState(state);
    }
    virtual void setApplicationValue(const char *app, const char *propname, const char *value, bool overwrite)
    {
        VStringBuffer xpath("Application/%s/%s", app, propname);
        if (trackSecondaryChange(value, xpath))
            CLocalWorkUnit::setApplicationValue(app, propname, value, overwrite);
    }

    virtual void _lockRemote()
    {
        // Ignore locking for now!
//        printStackReport();
//        UNIMPLEMENTED;
        batch.setown(new CassandraBatch(cass_batch_new(CASS_BATCH_TYPE_UNLOGGED)));
    }

    virtual void _unlockRemote()
    {
//        printStackReport();
//        UNIMPLEMENTED;
        commit();
        batch.clear();
    }

    virtual void subscribe(WUSubscribeOptions options)
    {
//        printStackReport();
//        UNIMPLEMENTED;
    }

    virtual void unsubscribe()
    {
//        printStackReport();
//        UNIMPLEMENTED;
    }

    virtual bool aborting() const
    {
        return false;
        // MORE - work out what to do about aborts in Cassandra
//        printStackReport();
//        UNIMPLEMENTED;
    }

    virtual IWUResult * updateResultByName(const char * name)
    {
        return noteDirty(CLocalWorkUnit::updateResultByName(name));
    }
    virtual IWUResult * updateResultBySequence(unsigned seq)
    {
        return noteDirty(CLocalWorkUnit::updateResultBySequence(seq));
    }
    virtual IWUResult * updateTemporaryByName(const char * name)
    {
        return noteDirty(CLocalWorkUnit::updateTemporaryByName(name));
    }
    virtual IWUResult * updateVariableByName(const char * name)
    {
        return noteDirty(CLocalWorkUnit::updateVariableByName(name));
    }
    virtual IWUException *createException()
    {
        IWUException *result = CLocalWorkUnit::createException();
        VStringBuffer xpath("Exceptions/Exception[@sequence='%d']", result->getSequence());
        noteDirty(xpath, wuExceptionsMappings);
        return result;
    }
    virtual void copyWorkUnit(IConstWorkUnit *cached, bool all)
    {
        // Make sure that any required updates to the secondary files happen
        IPropertyTree *fromP = queryExtendedWU(cached)->queryPTree();
        for (const char * const *search = searchPaths; *search; search++)
            trackSecondaryChange(fromP->queryProp(*search), *search);
        for (const ChildTableInfo * const * table = childTables; *table != NULL; table++)
            checkChildLoaded(**table);
        CLocalWorkUnit::copyWorkUnit(cached, all);
        memset(childLoaded, 1, sizeof(childLoaded));
        allDirty = true;
    }
    virtual void noteFileRead(IDistributedFile *file)
    {
        if (file)
        {
            CLocalWorkUnit::noteFileRead(file);
            VStringBuffer xpath("FilesRead/File[@name='%s']", file->queryLogicalName());
            noteDirty(xpath, wuFilesReadMappings);
        }
        else
        {
            // A hack for testing!
            Owned<IPropertyTreeIterator> files = p->getElements("FilesRead/File");
            ForEach(*files)
            {
                VStringBuffer xpath("FilesRead/File[@name='%s']", files->query().queryProp("@name"));
                noteDirty(xpath, wuFilesReadMappings);
            }
        }
    }

    virtual void _loadFilesRead() const
    {
        checkChildLoaded(wuFilesReadTable);        // Lazy populate the FilesRead branch of p from Cassandra
        CLocalWorkUnit::_loadFilesRead();
    }

    virtual void _loadResults() const
    {
        checkChildLoaded(wuResultsTable);        // Lazy populate the Results branch of p from Cassandra
        CLocalWorkUnit::_loadResults();
    }

    virtual void _loadVariables() const
    {
        checkChildLoaded(wuVariablesTable);        // Lazy populate the Variables branch of p from Cassandra
        CLocalWorkUnit::_loadVariables();
    }

    virtual void _loadTemporaries() const
    {
        checkChildLoaded(wuTemporariesTable);        // Lazy populate the Temporaries branch of p from Cassandra
        CLocalWorkUnit::_loadTemporaries();
    }

    virtual void _loadStatistics() const
    {
        checkChildLoaded(wuStatisticsTable);        // Lazy populate the Statistics branch of p from Cassandra
        CLocalWorkUnit::_loadStatistics();
    }

    virtual void _loadExceptions() const
    {
        checkChildLoaded(wuExceptionsTable);        // Lazy populate the Exceptions branch of p from Cassandra
        CLocalWorkUnit::_loadExceptions();
    }

    virtual void clearExceptions()
    {
        CriticalBlock b(crit);
        noteDirty("*Exceptions/Exception", wuExceptionsMappings);
        CLocalWorkUnit::clearExceptions();
    }

    virtual IPropertyTree *queryPTree() const
    {
        // If anyone wants the whole ptree, we'd better make sure we have fully loaded it...
        CriticalBlock b(crit);
        for (const ChildTableInfo * const * table = childTables; *table != NULL; table++)
            checkChildLoaded(**table);
        return p;
    }
protected:
    // Delete child table rows
    void deleteChildren(const char *wuid)
    {
        for (const ChildTableInfo * const * table = childTables; *table != NULL; table++)
            sessionCache->deleteChildByWuid(table[0]->mappings, wuid, *batch);
    }

    // Lazy-populate a portion of WU xml from a child table
    void checkChildLoaded(const ChildTableInfo &childTable) const
    {
        // NOTE - should be called inside critsec
        if (!childLoaded[childTable.index])
        {
            CassandraResult result(sessionCache->fetchDataForWuid(childTable.mappings, queryWuid(), false));
            Owned<IPTree> results;
            CassandraIterator rows(cass_iterator_from_result(result));
            while (cass_iterator_next(rows))
            {
                CassandraIterator cols(cass_iterator_from_row(cass_iterator_get_row(rows)));
                Owned<IPTree> child;
                if (!results)
                    results.setown(createPTree(childTable.parentElement));
                child.setown(createPTree(childTable.childElement));
                unsigned colidx = 2;  // We did not fetch wuid or partition
                while (cass_iterator_next(cols))
                {
                    assertex(childTable.mappings[colidx].columnName);
                    const CassValue *value = cass_iterator_get_column(cols);
                    if (value && !cass_value_is_null(value))
                        childTable.mappings[colidx].mapper.toXML(child, childTable.mappings[colidx].xpath, value);
                    colidx++;
                }
                const char *childName = child->queryName();
                results->addPropTree(childName, child.getClear());
            }
            if (results)
                p->addPropTree(childTable.parentElement, results.getClear());
            childLoaded[childTable.index] = true;
        }
    }

    // Update secondary tables (used to search wuids by owner, state, jobname etc)

    void updateSecondaryTable(const char *xpath, const char *prevKey, const char *wuid)
    {
        if (prevKey && *prevKey)
            deleteSecondaryByKey(xpath, prevKey, wuid, sessionCache, *batch);
        if (p->hasProp(xpath))
            simpleXMLtoCassandra(sessionCache, *batch, searchMappings, p, xpath);
    }

    void deleteAppSecondaries(IPTree &pt, const char *wuid)
    {
        Owned<IPTreeIterator> apps = pt.getElements("Application");
        ForEach(*apps)
        {
            IPTree &app = apps->query();
            Owned<IPTreeIterator> names = app.getElements("*");
            ForEach(*names)
            {
                IPTree &name = names->query();
                Owned<IPTreeIterator> values = name.getElements("*");
                ForEach(*values)
                {
                    IPTree &value = values->query();
                    const char *appValue = value.queryProp(".");
                    if (appValue && *appValue)
                    {
                        VStringBuffer xpath("%s/%s/%s", app.queryName(), name.queryName(), value.queryName());
                        deleteSecondaryByKey(xpath, appValue, wuid, sessionCache, *batch);
                    }
                }
            }
        }
    }

    void deleteSecondaries(const char *wuid)
    {
        for (const char * const *search = searchPaths; *search; search++)
            deleteSecondaryByKey(*search, p->queryProp(*search), wuid, sessionCache, *batch);
        deleteAppSecondaries(*p, wuid);
        Owned<IPropertyTreeIterator> filesRead = &getFilesReadIterator();
        ForEach(*filesRead)
        {
            IPTree &file = filesRead->query();
            deleteSimpleXML(sessionCache, *batch, filesReadSearchMappings, &file, wuid);
        }
        // MORE deleteFilesReadSecondaries(*p, wuid);
    }

    void updateSecondaries(const char *wuid)
    {
        const char * const *search;
        for (search = searchPaths; *search; search++)
            updateSecondaryTable(*search, prev->queryProp(*search), wuid);
        for (search = wildSearchPaths; *search; search++)
        {
            const char *value = p->queryProp(*search);
            if (value)
                addUniqueValue(sessionCache, *batch, *search, value);
        }
        deleteAppSecondaries(*prev, wuid);
        Owned<IConstWUAppValueIterator> appValues = &getApplicationValues();
        ForEach(*appValues)
        {
            IConstWUAppValue& val=appValues->query();
            addUniqueValue(sessionCache, *batch, "Application", val.queryApplication());  // Used to populate droplists of applications
            VStringBuffer key("@@%s", val.queryApplication());
            addUniqueValue(sessionCache, *batch, key, val.queryName());  // Used to populate droplists of value names for a given application
            VStringBuffer xpath("Application/%s/%s", val.queryApplication(), val.queryName());
            addUniqueValue(sessionCache, *batch, xpath, val.queryValue());  // Used to get lists of values for a given app and name, and for filtering
            simpleXMLtoCassandra(sessionCache, *batch, searchMappings, p, xpath);
        }
        Owned<IPropertyTreeIterator> filesRead = &getFilesReadIterator();
        ForEach(*filesRead)
        {
            IPTree &file = filesRead->query();
            simpleXMLtoCassandra(sessionCache, *batch, filesReadSearchMappings, &file, wuid);
        }
    }

    // Keep track of previously committed values for fields that we have a secondary table for, so that we can update them appropriately when we commit

    bool trackSecondaryChange(const char *newval, const char *xpath)
    {
        if (!newval)
            newval = "";
        const char *oldval = p->queryProp(xpath);
        if (!oldval)
             oldval = "";
        if (streq(newval, oldval))
            return false;  // No change
        bool add;
        if (!prev)
        {
            prev.setown(createPTree());
            add = true;
        }
        else add = !prev->hasProp(xpath);
        if (add)
        {
            const char *tailptr = strrchr(xpath, '/');
            if (tailptr)
            {
                StringBuffer head(tailptr-xpath, xpath);
                ensurePTree(prev, head)->setProp(tailptr+1, oldval);
            }
            else
                prev->setProp(xpath, oldval);
        }
        return true;
    }
    IWUResult *noteDirty(IWUResult *result)
    {
        if (result)
            dirtyResults.append(*LINK(result));
        return result;
    }

    void noteDirty(const char *xpath, const CassandraXmlMapping *table)
    {
        dirtyPaths.setValue(xpath, table);
    }
    Linked<const ICassandraSession> sessionCache;
    mutable bool abortDirty;
    mutable bool abortState;
    mutable bool childLoaded[ChildTablesSize];
    bool allDirty;
    Owned<IPTree> prev;

    Owned<CassandraBatch> batch;
    MapStringTo<const CassandraXmlMapping *> dirtyPaths;
    IArrayOf<IWUResult> dirtyResults;
};

class CCasssandraWorkUnitFactory : public CWorkUnitFactory, implements ICassandraSession
{
    IMPLEMENT_IINTERFACE;
public:
    CCasssandraWorkUnitFactory(const IPropertyTree *props) : cluster(cass_cluster_new()), randomizeSuffix(0), randState((unsigned) get_cycles_now()), cacheRetirer(*this)
    {
        StringArray options;
        Owned<IPTreeIterator> it = props->getElements("Option");
        ForEach(*it)
        {
            IPTree &item = it->query();
            const char *opt = item.queryProp("@name");
            const char *val = item.queryProp("@value");
            if (opt && val)
            {
                if (strieq(opt, "randomWuidSuffix"))
                    randomizeSuffix = atoi(val);
                else if (strieq(opt, "traceLevel"))
                    traceLevel = atoi(val);
                else
                {
                    VStringBuffer optstr("%s=%s", opt, val);
                    options.append(optstr);
                }
            }
        }
        cluster.setOptions(options);
        if (cluster.keyspace.isEmpty())
            cluster.keyspace.set("hpcc");
        connect();
        cacheRetirer.start();
    }

    ~CCasssandraWorkUnitFactory()
    {
        cacheRetirer.stop();
        cacheRetirer.join();
    }

    virtual CLocalWorkUnit* _createWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
    {
        unsigned suffix;
        unsigned suffixLength;
        if (randomizeSuffix)  // May need to enable this option if you are expecting to create hundreds of workunits / second
        {
            suffix = rand_r(&randState);
            suffixLength = randomizeSuffix;
        }
        else
        {
            suffix = 0;
            suffixLength = 0;
        }
        Owned<CassandraPrepared> prepared = prepareStatement("INSERT INTO workunits (partition, wuid) VALUES (?,?) IF NOT EXISTS;");
        loop
        {
            // Create a unique WUID by adding suffixes until we managed to add a new value
            StringBuffer useWuid(wuid);
            if (suffix)
            {
                useWuid.append("-");
                for (unsigned i = 0; i < suffixLength; i++)
                {
                    useWuid.appendf("%c", '0'+suffix%10);
                    suffix /= 10;
                }
            }
            CassandraStatement statement(cass_prepared_bind(*prepared));
            check(cass_statement_bind_int32(statement, 0, rtlHash32VStr(useWuid.str(), 0) % NUM_PARTITIONS));
            statement.bindString(1, useWuid.str());
            if (traceLevel >= 2)
                DBGLOG("Try creating %s", useWuid.str());
            CassandraFuture future(cass_session_execute(session, statement));
            future.wait("execute");
            CassandraResult result(cass_future_get_result(future));
            if (cass_result_column_count(result)==1)
            {
                // A single column result indicates success, - the single column should be called '[applied]' and have the value 'true'
                // If there are multiple columns it will be '[applied]' (value false) and the fields of the existing row
                Owned<IPTree> wuXML = createPTree(useWuid);
                wuXML->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
                Owned<CLocalWorkUnit> wu = new CCassandraWorkUnit(this, wuXML.getClear(), secmgr, secuser);
                wu->lockRemote(true);
                return wu.getClear();
            }
            suffix = rand_r(&randState);
            if (suffixLength<9)
                suffixLength++;
        }
    }
    virtual CLocalWorkUnit* _openWorkUnit(const char *wuid, bool lock, ISecManager *secmgr, ISecUser *secuser)
    {
        // MORE - what to do about lock?
        Owned<IPTree> wuXML = cassandraToWorkunitXML(wuid);
        if (wuXML)
            return new CCassandraWorkUnit(this, wuXML.getClear(), secmgr, secuser);
        else
            return NULL;
    }
    virtual CLocalWorkUnit* _updateWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
    {
        // Ignore locking for now
        // Note - in Dali, this would lock for write, whereas _openWorkUnit would either lock for read (if lock set) or not lock at all
        Owned<IPTree> wuXML = cassandraToWorkunitXML(wuid);
        Owned<CLocalWorkUnit> wu = new CCassandraWorkUnit(this, wuXML.getClear(), secmgr, secuser);
        wu->lockRemote(true);
        return wu.getClear();
    }

    virtual IWorkUnit * getGlobalWorkUnit(ISecManager *secmgr = NULL, ISecUser *secuser = NULL) { UNIMPLEMENTED; }
    virtual IConstWorkUnitIterator * getWorkUnitsByOwner(const char * owner, ISecManager *secmgr, ISecUser *secuser)
    {
        return getWorkUnitsByXXX("@submitID", owner, secmgr, secuser);
    }
    virtual IConstWorkUnitIterator * getScheduledWorkUnits(ISecManager *secmgr, ISecUser *secuser)
    {
        return getWorkUnitsByXXX("@state", getWorkunitStateStr(WUStateScheduled), secmgr, secuser); // MORE - there may be more efficient ways to do this?
    }
    virtual IConstWorkUnitIterator * getWorkUnitsSorted(WUSortField sortorder, WUSortField * filters, const void * filterbuf,
                                                        unsigned startOffset, unsigned pageSize, __int64 * cachehint, unsigned *total,
                                                        ISecManager *secmgr, ISecUser *secuser)
    {
        // To assist in the efficient implementation of this function without requiring local sorting and filtering,
        // we maintain a couple of additional search tables in addition to the main workunit table.
        //
        // The workunitsSearch table allows us to map from a given field's value to a workunit - to avoid the need
        // for a second lookup this table contains a copy of all the 'lightweight' fields in the workunit. The table
        // has a partition key of xpath, searchPrefix allowing it to be used for range lookups provided at least
        // 2 characters are provided, while hopefully spreading the load a little between Cassandra partitions.
        //
        // The uniqueValues table is used to track what values are present for some wild-searchable fields, so we do
        // two lookups - one to translate the wildcard to a set, then others to retrieve the wus matching each value
        // in the set. These are done as N parallel reads rather than a single query (which might naively be expected
        // to be more efficient) for two reasons. Firstly, we can get them back sorted that way and merge the results
        // on the fly. Secondly, it is actually more efficient, at least in the case when there are multiple Cassandra
        // partitions, since it in-effect cuts out the step of talking to a coordinator node which would talk to
        // multiple other nodes to get the data.
        //
        // We go to some lengths to avoid post-sorting if we can, but any sort order other than by wuid or totalThorTime
        // will post-sort it. If a post-sort is required, we will fetch up to WUID_LOCALSORT_LIMIT rows, - if there are
        // more then we should fail, and the user should be invited to add filters.
        //
        // We can do at most one 'hard' filter, plus a filter on wuid range - anything else will require post-filtering.
        // Most 'wild' searches can only be done with post-filtering, but some can be translated to multiple hard values
        // using the unique values table. In such cases we merge results in the fly to avoid a post-sort if possible
        //
        // Note that Cassandra does not presently support filtering before returning the values except where a
        // key or secondary index is available - even if ALLOW FILTERING is specified. If it did, some of the post-
        // filtering would be better off done at the Cassandra side.
        //
        // We should encourage the UI to present drop-lists of users for filtering, to avoid the use of wildcard
        // searches just because people can't remember the name.
        //
        // Searching by files probably needs to be done differently - a separate table mapping filenames to wuids.
        // This can perhaps be join-merged if other filters are present. This is still TBD at the moment.

        Owned<CCassandraWuUQueryCacheEntry> cached;
        if (cachehint && *cachehint)
        {
            CriticalBlock b(cacheCrit);
            cached.set(cacheIdMap.getValue(*cachehint));
        }
        if (cached)
            cached->touch();
        else
            cached.setown(new CCassandraWuUQueryCacheEntry());
        const WUSortField *thisFilter = filters;
        IArrayOf<IPostFilter> goodFilters;
        IArrayOf<IPostFilter> wuidFilters;
        IArrayOf<IPostFilter> poorFilters;
        IArrayOf<IPostFilter> fileFilters;
        IArrayOf<IPostFilter> remoteWildFilters;
        Owned<IConstWorkUnitIteratorEx> result;
        WUSortField baseSort = (WUSortField) (sortorder & 0xff);
        StringBuffer thorTimeThreshold;
        bool sortByThorTime = (baseSort == WUSFtotalthortime);
        bool needsPostSort = (baseSort != WUSFwuid && baseSort != WUSFtotalthortime);
        bool sortDescending = (sortorder & WUSFreverse) || needsPostSort;
        if (!result)
        {
            Owned<CassMultiIterator> merger = new CassMultiIterator(needsPostSort ? NULL : cached, 0, 0, sortDescending); // We always merge by wuid // MORE - except when we merge by thor time....
            if (startOffset)
            {
                StringBuffer startWuid;
                unsigned found = cached->lookupStartRow(startWuid, thorTimeThreshold, startOffset);
                if (found)
                {
                    if (!sortByThorTime)
                    {
                        if (sortDescending)
                            startWuid.setCharAt(startWuid.length()-1, startWuid.charAt(startWuid.length()-1)-1);  // we want to find the last wuid BEFORE
                        else
                            startWuid.append('\x21');  // we want to find the first wuid AFTER. This is printable but not going to be in any wuid
                        thorTimeThreshold.clear();
                    }
                    wuidFilters.append(*new PostFilter(sortorder==WUSFwuid ? WUSFwuid : WUSFwuidhigh, startWuid, true));
                    startOffset -= found;
                    merger->setStartOffset(found);
                }
            }
            const char *fv = (const char *) filterbuf;
            while (thisFilter && *thisFilter)
            {
                WUSortField field = (WUSortField) (*thisFilter & 0xff);
                bool isWild = (*thisFilter & WUSFwild) != 0;

                switch (field)
                {
                case WUSFappvalue:
                {
                    const char *name = fv;
                    fv = fv + strlen(fv)+1;
                    if (isWild)
                    {
                        StringBuffer s(fv);
                        if (s.charAt(s.length()-1)== '*')
                            s.remove(s.length()-1, 1);
                        if (s.length())
                            remoteWildFilters.append(*new AppValuePostFilter(name, s, true)); // Should we allow wild on the app and/or name too? Not at the moment
                    }
                    else
                        goodFilters.append(*new AppValuePostFilter(name, fv, false));
                    break;
                }
                case WUSFuser:
                case WUSFcluster:
                case WUSFjob:
                    if (isWild)
                    {
                        StringBuffer s(fv);
                        if (s.charAt(s.length()-1)== '*')
                            s.remove(s.length()-1, 1);
                        if (s.length())
                            remoteWildFilters.append(*new PostFilter(field, s, true));  // Trailing-only wildcards can be done remotely
                    }
                    else
                        goodFilters.append(*new PostFilter(field, fv, false));
                    break;
                case WUSFstate:
                case WUSFpriority:
                case WUSFprotected:
                    // These can't be wild, but are not very good filters
                    poorFilters.append(*new PostFilter(field, fv, false));
                    break;
                case WUSFwuid: // Acts as wuidLo when specified as a filter
                case WUSFwuidhigh:
                    // Wuid filters can be added to good and poor filters, and to remoteWild if they are done via merged sets rather than ranges...
                    if (sortByThorTime)
                        remoteWildFilters.append(*new PostFilter(field, fv, true));
                    else
                        mergeFilter(wuidFilters, field, fv);
                    break;
                case WUSFfileread:
                    fileFilters.append(*new PostFilter(field, fv, true));
                    break;
                case WUSFtotalthortime:
                    // This should be treated as a low value - i.e. return only wu's that took longer than the supplied value
                    if (thorTimeThreshold.isEmpty()) // If not a continuation
                        formatTimeCollatable(thorTimeThreshold, milliToNano(atoi(fv)), false);
                    break;
                case WUSFwildwuid:
                    // Translate into a range - note that we only support trailing * wildcard.
                    if (fv && *fv)
                    {
                        StringBuffer s(fv);
                        if (s.charAt(s.length()-1)== '*')
                            s.remove(s.length()-1, 1);
                        if (s.length())
                        {
                            mergeFilter(wuidFilters, WUSFwuid, s);
                            s.append('\x7e');  // '~' - higher than anything that should occur in a wuid (but still printable)
                            mergeFilter(wuidFilters, WUSFwuidhigh, s);
                        }
                    }
                    break;
                case WUSFecl: // This is different...
                    if (isWild)
                        merger->addPostFilter(*new PostFilter(field, fv, true)); // Wildcards on ECL are trailing and leading - no way to do remotely
                    else
                        goodFilters.append(*new PostFilter(field, fv, false)); // A hard filter on exact ecl match is possible but very unlikely
                default:
                    UNSUPPORTED("Workunit filter criteria");
                }
                thisFilter++;
                fv = fv + strlen(fv)+1;
            }
            if (fileFilters.length())
            {
                // We can't postfilter by these - we COULD in some cases do a join between these and some other filtered set
                // but we will leave that as an exercise to the reader. So if there is a fileFilter, read it first, and turn it into a merge set of the resulting wus.
                assertex(fileFilters.length()==1);  // If we supported more there would be a join phase here
                merger->addPostFilters(goodFilters, 0);
                merger->addPostFilters(poorFilters, 0);
                merger->addPostFilters(remoteWildFilters, 0);
                CassandraResult wuids(fetchDataForFileRead(fileFilters.item(0).queryValue(), wuidFilters, 0));
                CassandraIterator rows(cass_iterator_from_result(wuids));
                StringBuffer value;
                while (cass_iterator_next(rows))
                {
                    const CassRow *row = cass_iterator_get_row(rows);
                    getCassString(value.clear(), cass_row_get_column(row, 0));
                    merger->addResult(*new CassandraResult(fetchDataForWuid(workunitInfoMappings, value, true)));
                }
            }
            else if (sortByThorTime)
            {
                merger->addPostFilters(goodFilters, 0);
                merger->addPostFilters(poorFilters, 0);
                merger->addPostFilters(remoteWildFilters, 0);
                if (wuidFilters.length())
                {
                    // We are doing a continuation of a prior search that is sorted by a searchField, which may not be unique
                    // We need two queries - one where searchField==startSearchField and wuid > startWuid,
                    // and one where searchField > startSearchField. We know that there are no other filters in play (as Cassandra would not support them)
                    // though there may be postfilters
                    assertex(wuidFilters.length()==1);
                    merger->addResult(*new CassandraResult(fetchMoreDataByThorTime(thorTimeThreshold, wuidFilters.item(0).queryValue(), sortDescending, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
                    merger->addResult(*new CassandraResult(fetchMoreDataByThorTime(thorTimeThreshold, NULL, sortDescending, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
                    merger->setCompareColumn(-1);  // we want to preserve the order of these two results
                }
                else
                    merger->addResult(*new CassandraResult(fetchDataByThorTime(thorTimeThreshold, sortDescending, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
            }
            else if (goodFilters.length())
            {
                merger->addPostFilters(goodFilters, 1);
                merger->addPostFilters(poorFilters, 0);
                merger->addPostFilters(remoteWildFilters, 0);
                const IPostFilter &best = goodFilters.item(0);
                merger->addResult(*new CassandraResult(fetchDataForKeyWithFilter(best.queryXPath(), best.queryValue(), wuidFilters, sortorder, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
            }
            else if (poorFilters.length())
            {
                merger->addPostFilters(poorFilters, 1);
                merger->addPostFilters(remoteWildFilters, 0);
                const IPostFilter &best= poorFilters.item(0);
                merger->addResult(*new CassandraResult(fetchDataForKeyWithFilter(best.queryXPath(), best.queryValue(), wuidFilters, sortorder, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
            }
            else if (remoteWildFilters.length())
            {
                merger->addPostFilters(remoteWildFilters, 1);  // Any other filters have to be done locally
                // Convert into a value IN [] which we do via a merge
                // MORE - If we want sorted by filter (or don't care about sort order), we could do directly as a range - but the wuid range filters then don't work, and the merger would be invalid
                StringArray fieldValues;
                const IPostFilter &best= remoteWildFilters.item(0);
                _getUniqueValues(best.queryXPath(), best.queryValue(), fieldValues);
                ForEachItemIn(idx, fieldValues)
                {
                    merger->addResult(*new CassandraResult(fetchDataForKeyWithFilter(best.queryXPath(), fieldValues.item(idx), wuidFilters, sortorder, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
                }
            }
            else
            {
                // If all we have is a wuid range (or nothing), search the wuid table and/or return everything
                for (int i = 0; i < NUM_PARTITIONS; i++)
                {
                    merger->addResult(*new CassandraResult(fetchDataByPartition(workunitInfoMappings, i, wuidFilters, sortorder, merger->hasPostFilters() ? 0 : pageSize+startOffset)));
                }
            }

            // The result we have will be sorted by wuid (ascending or descending)
            if (needsPostSort)
            {
                // A post-sort will be required.
                // Result should be limited in (to CASS_WORKUNIT_POSTSORT_LIMIT * number of results being merged)
                result.setown(new CassPostSortIterator(merger.getClear(), sortorder, pageSize > CASS_WORKUNIT_POSTSORT_LIMIT ? pageSize : CASS_WORKUNIT_POSTSORT_LIMIT));
                cached->setResult(result);
            }
            else
                result.setown(merger.getClear());
        }
        if (startOffset || needsPostSort || result->hasPostFilters() || result->isMerging()) // we need a subpage if we have fetched anything other than exactly the rows requested
            result.setown(new SubPageIterator(result.getClear(), startOffset, pageSize));
        if (cachehint)
        {
            *cachehint = cached->queryHint();
            CriticalBlock b(cacheCrit);
            cacheIdMap.setValue(*cachehint, cached.getClear());
        }
        if (total)
            *total = 0; // We don't know
        return result.getClear();
    }
    virtual StringArray &getUniqueValues(WUSortField field, const char *prefix, StringArray &result) const
    {
        return _getUniqueValues(queryFilterXPath(field), prefix, result);
    }
    virtual unsigned numWorkUnits()
    {
        Owned<CassandraPrepared> prepared = prepareStatement("SELECT COUNT(*) FROM workunits;");
        CassandraStatement statement(cass_prepared_bind(*prepared));
        CassandraFuture future(cass_session_execute(session, statement));
        future.wait("select count(*)");
        CassandraResult result(cass_future_get_result(future));
        return getUnsignedResult(NULL, getSingleResult(result));
    }
    /*
    virtual void descheduleAllWorkUnits(ISecManager *secmgr, ISecUser *secuser) { UNIMPLEMENTED; }
    virtual IConstQuerySetQueryIterator * getQuerySetQueriesSorted(WUQuerySortField *sortorder, WUQuerySortField *filters, const void *filterbuf, unsigned startoffset, unsigned maxnum, __int64 *cachehint, unsigned *total, const MapStringTo<bool> *subset) { UNIMPLEMENTED; }
    virtual bool isAborting(const char *wuid) const { UNIMPLEMENTED; }
    virtual void clearAborting(const char *wuid) { UNIMPLEMENTED; }
    */
    virtual WUState waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, bool returnOnWaitState)
    {
        VStringBuffer select("select state from workunits where wuid = '%s';", wuid);
        CassandraStatement statement(cass_statement_new(select.str(), 0));
        unsigned start = msTick();
        loop
        {
            CassandraFuture future(cass_session_execute(session, statement));
            future.wait("Lookup wu state");
            CassandraResult result(cass_future_get_result(future));
            const CassValue *value = getSingleResult(result);
            if (value == NULL)
                return WUStateUnknown;
            const char *output;
            size_t length;
            check(cass_value_get_string(value, &output, &length));
            StringBuffer stateStr(length, output);
            WUState state = getWorkUnitState(stateStr);
            switch (state)
            {
            case WUStateCompiled:
            case WUStateUploadingFiles:
                if (compiled)
                    return state;
                break;
            case WUStateCompleted:
            case WUStateFailed:
            case WUStateAborted:
                return state;
            case WUStateWait:
                if (returnOnWaitState)
                    return state;
                break;
            case WUStateCompiling:
            case WUStateRunning:
            case WUStateDebugPaused:
            case WUStateDebugRunning:
            case WUStateBlocked:
            case WUStateAborting:
                // MORE - can see if agent still running, and set to failed if it is not
                break;
            }
            unsigned waited = msTick() - start;
            if (timeout != -1 && waited > timeout)
            {
                return WUStateUnknown;
                break;
            }
            Sleep(1000); // MORE - may want to back off as waited gets longer...
        }
    }

    unsigned validateRepository(bool fix)
    {
        unsigned errCount = 0;
        // MORE - if the batch gets too big you may need to flush it occasionally
        CassandraBatch batch(fix ? cass_batch_new(CASS_BATCH_TYPE_LOGGED) : NULL);
        // 1. Check that every entry in main wu table has matching entries in secondary tables
        CassandraResult result(fetchData(workunitInfoMappings+1));
        CassandraIterator rows(cass_iterator_from_result(result));
        if (batch)
        {
            // Delete the unique values table - the validate process recreates it afresh
            CassandraStatement truncate(cass_statement_new("TRUNCATE uniqueSearchValues", 0));
            check(cass_batch_add_statement(batch, truncate));
        }
        while (cass_iterator_next(rows))
        {
            Owned<IPTree> wuXML = rowToPTree(NULL, NULL, workunitInfoMappings+1, cass_iterator_get_row(rows));
            const char *wuid = wuXML->queryName();
            // For each search entry, check that we get matching XML
            for (const char * const *search = searchPaths; *search; search++)
                errCount += validateSearch(*search, wuid, wuXML, batch);
        }
        // 2. Check that there are no orphaned entries in search or child tables
        errCount += checkOrphans(searchMappings, 3, batch);
        for (const ChildTableInfo * const * table = childTables; *table != NULL; table++)
            errCount += checkOrphans(table[0]->mappings, 1, batch);
        // 3. Commit fixes
        if (batch)
        {
            CassandraFuture futureBatch(cass_session_execute_batch(session, batch));
            futureBatch.wait("Fix_repository");
        }
        return errCount;
    }

    virtual void deleteRepository(bool recreate)
    {
        // USE WITH CARE!
        session.set(cass_session_new());
        CassandraFuture future(cass_session_connect(session, cluster));
        future.wait("connect without keyspace to delete");
        VStringBuffer deleteKeyspace("DROP KEYSPACE IF EXISTS %s;", cluster.keyspace.get());
        executeSimpleCommand(session, deleteKeyspace);
        if (recreate)
            connect();
        else
            session.set(NULL);
    }

    virtual void createRepository()
    {
        session.set(cass_session_new());
        CassandraFuture future(cass_session_connect(session, cluster));
        future.wait("connect without keyspace");
        VStringBuffer create("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;", cluster.keyspace.get()); // MORE - options from props? Not 100% sure if they are appropriate.
        executeSimpleCommand(session, create);
        connect();
        ensureTable(session, workunitsMappings);
        ensureTable(session, searchMappings);
        ensureTable(session, uniqueSearchMappings);
        ensureTable(session, filesReadSearchMappings);
        for (const ChildTableInfo * const * table = childTables; *table != NULL; table++)
            ensureTable(session, table[0]->mappings);
    }

    virtual const char *queryStoreType() const
    {
        return "Cassandra";
    }

    // Interface ICassandraSession
    virtual CassSession *querySession() const { return session; };
    virtual unsigned queryTraceLevel() const { return traceLevel; };
    virtual CassandraPrepared *prepareStatement(const char *query) const
    {
        assertex(session);
        CriticalBlock b(cacheCrit);
        Linked<CassandraPrepared> cached = preparedCache.getValue(query);
        if (cached)
        {
            if (traceLevel >= 2)
                DBGLOG("prepareStatement: Reusing %s", query);
            return cached.getClear();
        }
        {
            if (traceLevel >= 2)
                DBGLOG("prepareStatement: Binding %s", query);
            // We don't want to block cache lookups while we prepare a new bound statement
            // Note - if multiple threads try to prepare the same (new) statement at the same time, it's not catastrophic
            CriticalUnblock b(cacheCrit);
            CassandraFuture futurePrep(cass_session_prepare(session, query));
            futurePrep.wait("prepare statement");
            cached.setown(new CassandraPrepared(cass_future_get_prepared(futurePrep)));
        }
        preparedCache.setValue(query, cached); // NOTE - this links parameter
        return cached.getClear();
    }
private:
    void connect()
    {
        session.set(cass_session_new());
        CassandraFuture future(cass_session_connect_keyspace(session, cluster, cluster.keyspace));
        future.wait("connect with keyspace");
    }
    bool checkWuExists(const char *wuid)
    {
        Owned<CassandraPrepared> prepared = prepareStatement("SELECT COUNT(*) FROM workunits where partition=? and wuid=?;");
        CassandraStatement statement(cass_prepared_bind(*prepared));
        cass_statement_bind_int32(statement, 0, rtlHash32VStr(wuid, 0) % NUM_PARTITIONS);
        cass_statement_bind_string(statement, 1, wuid);
        CassandraFuture future(cass_session_execute(session, statement));
        future.wait("select count(*)");
        CassandraResult result(cass_future_get_result(future));
        return getUnsignedResult(NULL, getSingleResult(result)) != 0; // Shouldn't be more than 1, either
    }
    void mergeFilter(IArrayOf<IPostFilter> &filters, WUSortField field, const char *value)
    {
        // Combine multiple filters on wuid - Cassandra doesn't like seeing more than one.
        ForEachItemIn(idx, filters)
        {
            PostFilter &filter = static_cast<PostFilter &>(filters.item(idx));
            if (filter.queryField()==field)
            {
                const char *prevLimit = filter.queryValue();
                int diff = strcmp(prevLimit, value);
                if (diff && ((diff < 0) == (field==WUSFwuid)))
                    filter.setValue(value);
                return;
            }
        }
        // Not found - add new filter
        filters.append(*new PostFilter(field, value, true));
    }
    IConstWorkUnitIterator * getWorkUnitsByXXX(const char *xpath, const char *key, ISecManager *secmgr, ISecUser *secuser)
    {
        Owned<CassMultiIterator> merger = new CassMultiIterator(NULL, 0, 0, true); // Merge by wuid
        if (!key || !*key)
        {
            IArrayOf<IPostFilter> wuidFilters;
            for (int i = 0; i < NUM_PARTITIONS; i++)
            {
                merger->addResult(*new CassandraResult(fetchDataByPartition(workunitInfoMappings, i, wuidFilters)));
            }
        }
        else
            merger->addResult(*new CassandraResult(fetchDataForKey(xpath, key)));
        return createSecureConstWUIterator(merger.getClear(), secmgr, secuser);
    }
    StringArray &_getUniqueValues(const char *xpath, const char *prefix, StringArray &result) const
    {
        if (prefix && strlen(prefix) >= CASS_SEARCH_PREFIX_SIZE)
        {
            CassandraResult r(fetchDataForWildSearch(xpath, prefix, uniqueSearchMappings));
            CassandraIterator rows(cass_iterator_from_result(r));
            StringBuffer value;
            while (cass_iterator_next(rows))
            {
                const CassRow *row = cass_iterator_get_row(rows);
                getCassString(value.clear(), cass_row_get_column(row, 0));
                result.append(value);
            }
        }
        return result;
    }
    unsigned validateSearch(const char *xpath, const char *wuid, IPTree *wuXML, CassBatch *batch)
    {
        unsigned errCount = 0;
        const char *childKey = wuXML->queryProp(xpath);
        if (childKey && *childKey)
        {
            CassandraResult result(fetchDataForKeyAndWuid(xpath, childKey, wuid));
            if (batch)
                simpleXMLtoCassandra(this, batch, uniqueSearchMappings, wuXML, xpath);
            switch (cass_result_row_count(result))
            {
            case 0:
                DBGLOG("Missing search data for %s for wuid=%s key=%s", xpath, wuid, childKey);
                if (batch)
                    simpleXMLtoCassandra(this, batch, searchMappings, wuXML, xpath);
                errCount++;
                break;
            case 1:
            {
                Owned<IPTree> secXML = rowToPTree(xpath, childKey, searchMappings+4, cass_result_first_row(result));   // type, prefix, key, and wuid are not returned
                secXML->renameProp("/", wuid);
                if (!areMatchingPTrees(wuXML, secXML))
                {
                    DBGLOG("Mismatched search data for %s for wuid %s", xpath, wuid);
                    if (batch)
                        simpleXMLtoCassandra(this, batch, searchMappings, wuXML, xpath);
                    errCount++;
                }
                break;
            }
            default:
                DBGLOG("Multiple secondary data %d for %s for wuid %s", (int) cass_result_row_count(result), xpath, wuid); // This should be impossible!
                if (batch)
                {
                    deleteSecondaryByKey(xpath, childKey, wuid, this, batch);
                    simpleXMLtoCassandra(this, batch, searchMappings, wuXML, xpath);
                }
                break;
            }
        }
        return errCount;
    }

    unsigned checkOrphans(const CassandraXmlMapping *mappings, unsigned wuidIndex, CassBatch *batch)
    {
        unsigned errCount = 0;
        CassandraResult result(fetchData(mappings));
        CassandraIterator rows(cass_iterator_from_result(result));
        while (cass_iterator_next(rows))
        {
            const CassRow *row = cass_iterator_get_row(rows);
            StringBuffer wuid;
            getCassString(wuid, cass_row_get_column(row, wuidIndex));
            if (!checkWuExists(wuid))
            {
                DBGLOG("Orphaned data in %s for wuid=%s", queryTableName(mappings), wuid.str());
                if (batch)
                {
                    if (wuidIndex)
                    {
                        StringBuffer xpath, fieldValue;
                        getCassString(xpath, cass_row_get_column(row, 0));
                        getCassString(fieldValue, cass_row_get_column(row, 2));
                        deleteSecondaryByKey(xpath, fieldValue, wuid, this, batch);
                    }
                    else
                        deleteChildByWuid(mappings, wuid, batch);
                }
                errCount++;
            }
        }
        return errCount;
    }

    IPTree *cassandraToWorkunitXML(const char *wuid) const
    {
        CassandraResult result(fetchDataForWuid(workunitsMappings, wuid, false));
        CassandraIterator rows(cass_iterator_from_result(result));
        if (cass_iterator_next(rows)) // should just be one
        {
            Owned<IPTree> wuXML = createPTree(wuid);
            wuXML->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
            CassandraIterator cols(cass_iterator_from_row(cass_iterator_get_row(rows)));
            unsigned colidx = 2;  // wuid and partition are not returned
            while (cass_iterator_next(cols))
            {
                assertex(workunitsMappings[colidx].columnName);
                const CassValue *value = cass_iterator_get_column(cols);
                if (value && !cass_value_is_null(value))
                    workunitsMappings[colidx].mapper.toXML(wuXML, workunitsMappings[colidx].xpath, value);
                colidx++;
            }
            return wuXML.getClear();
        }
        else
            return NULL;
    }

    // Fetch all rows from a table

    const CassResult *fetchData(const CassandraXmlMapping *mappings) const
    {
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(mappings, names, tableName);
        VStringBuffer selectQuery("select %s from %s;", names.str()+1, tableName.str());
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch all rows from a single partition of a table

    const CassResult *fetchDataByPartition(const CassandraXmlMapping *mappings, int partition, const IArrayOf<IPostFilter> &wuidFilters, unsigned sortOrder=WUSFwuid|WUSFreverse, unsigned limit=0) const
    {
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(mappings+1, names, tableName); // Don't fetch partition column
        VStringBuffer selectQuery("select %s from %s where partition=%d", names.str()+1, tableName.str(), partition);
        ForEachItemIn(idx, wuidFilters)
        {
            const IPostFilter &wuidFilter = wuidFilters.item(idx);
            selectQuery.appendf(" and wuid %s '%s'", wuidFilter.queryField()==WUSFwuidhigh ? "<=" : ">=", wuidFilter.queryValue());
        }
        switch (sortOrder)
        {
        case WUSFwuid:
            selectQuery.append(" ORDER BY WUID ASC");
            break;
        case WUSFwuid|WUSFreverse:
            // If not wuid, descending, we will have to post-sort
            selectQuery.append(" ORDER BY WUID DESC");
            break;
        default:
            // If not wuid, descending, we will have to post-sort. We still need in wuid desc order for the merge though.
            selectQuery.append(" ORDER BY WUID DESC");
            if (!limit)
                limit = CASS_WORKUNIT_POSTSORT_LIMIT;
            break;
        }
        if (limit)
            selectQuery.appendf(" LIMIT %u", limit);
        selectQuery.append(';');
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch matching rows from a child table, or the main wu table

    const CassResult *fetchDataForWuid(const CassandraXmlMapping *mappings, const char *wuid, bool includeWuid) const
    {
        assertex(wuid && *wuid);
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(mappings + (includeWuid ? 1 : 2), names, tableName);  // mappings+2 means we don't return the partition or wuid columns
        VStringBuffer selectQuery("select %s from %s where partition=%d and wuid='%s';", names.str()+1, tableName.str(), rtlHash32VStr(wuid, 0) % NUM_PARTITIONS, wuid); // MORE - should consider using prepared/bind for this - is it faster?
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch matching rows from the search table, for all wuids, sorted by wuid

    const CassResult *fetchDataForKey(const char *xpath, const char *key) const
    {
        assertex(key);
        StringBuffer names;
        StringBuffer tableName;
        StringBuffer ucKey(key);
        ucKey.toUpperCase();
        getFieldNames(searchMappings+3, names, tableName);  // mappings+3 means we don't return the key columns (xpath, upper(keyPrefix), upper(key))
        VStringBuffer selectQuery("select %s from %s where xpath='%s' and fieldPrefix='%.*s' and fieldValue ='%s'", names.str()+1, tableName.str(), xpath, CASS_SEARCH_PREFIX_SIZE, ucKey.str(), ucKey.str()); // MORE - should consider using prepared/bind for this - is it faster?
        selectQuery.append(" ORDER BY fieldValue ASC, WUID desc;");
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch matching rows from the search table, for all wuids, sorted by wuid

    const CassResult *fetchDataForKeyWithFilter(const char *xpath, const char *key, const IArrayOf<IPostFilter> &wuidFilters, unsigned sortOrder, unsigned limit) const
    {
        StringBuffer names;
        StringBuffer tableName;
        StringBuffer ucKey(key);
        ucKey.toUpperCase();
        getFieldNames(searchMappings+3, names, tableName);  // mappings+3 means we don't return the key columns (xpath, upper(keyPrefix), upper(key))
        VStringBuffer selectQuery("select %s from %s where xpath='%s' and fieldPrefix='%.*s' and fieldValue ='%s'", names.str()+1, tableName.str(), xpath, CASS_SEARCH_PREFIX_SIZE, ucKey.str(), ucKey.str());
        ForEachItemIn(idx, wuidFilters)
        {
            const IPostFilter &wuidFilter = wuidFilters.item(idx);
            selectQuery.appendf(" and wuid %s '%s'", wuidFilter.queryField()==WUSFwuidhigh ? "<=" : ">=", wuidFilter.queryValue());
        }
        switch (sortOrder)
        {
        case WUSFwuid:
            selectQuery.append(" ORDER BY fieldValue DESC, WUID ASC");
            break;
        case WUSFwuid|WUSFreverse:
            selectQuery.append(" ORDER BY fieldValue ASC, WUID DESC");
            break;
        default:
            // If not wuid, descending, we will have to post-sort. We still need in wuid desc order for the merge though.
            selectQuery.appendf(" ORDER BY fieldvalue ASC, WUID DESC");
            limit = CASS_WORKUNIT_POSTSORT_LIMIT;
            break;
        }
        if (limit)
            selectQuery.appendf(" LIMIT %u", limit);
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch matching rows from the search or uniqueSearch table, for a given prefix

    const CassResult *fetchDataForWildSearch(const char *xpath, const char *prefix, const CassandraXmlMapping *mappings) const
    {
        assertex(prefix && *prefix);
        StringBuffer names;
        StringBuffer tableName;
        StringBuffer ucKey(prefix);
        ucKey.toUpperCase();
        StringBuffer ucKeyEnd(ucKey);
        size32_t len = ucKeyEnd.length();
        assertex(len);
        ucKeyEnd.setCharAt(len-1, ucKeyEnd.charAt(len-1)+1);
        getFieldNames(mappings+3, names, tableName);  // mappings+3 means we don't return the key columns (xpath, upper(keyPrefix), upper(key))
        VStringBuffer selectQuery("select %s from %s where xpath='%s' and fieldPrefix='%.*s' and fieldValue >='%s' and fieldValue < '%s';", names.str()+1, tableName.str(), xpath, CASS_SEARCH_PREFIX_SIZE, ucKey.str(), ucKey.str(), ucKeyEnd.str()); // MORE - should consider using prepared/bind for this - is it faster?
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch rows from the search table, by thorTime, above a threshold

    const CassResult *fetchDataByThorTime(const char *threshold, bool descending, unsigned limit) const
    {
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(searchMappings+3, names, tableName);  // mappings+3 means we don't return the key columns (xpath, upper(keyPrefix), upper(key))
        VStringBuffer selectQuery("select %s from %s where xpath='@totalThorTime' and fieldPrefix='%*s'", names.str()+1, tableName.str(), CASS_SEARCH_PREFIX_SIZE, "");
        if (threshold && *threshold)
            selectQuery.appendf(" where fieldValue >= '%s'", threshold);
        if (descending)
            selectQuery.append(" ORDER BY fieldValue DESC, wuid ASC");
        else
            selectQuery.append(" ORDER BY fieldValue ASC, wuid DESC");
        if (limit)
            selectQuery.appendf(" LIMIT %u", limit);
        selectQuery.append(';');
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch rows from the search table, continuing a previous query that was sorted by thor time - part one
    // This technique only works for thor time where we have forced to a single partition. Otherwise it gets even more complicated, and not worth it.

    const CassResult *fetchMoreDataByThorTime(const char *threshold, const char *wuid, bool descending, unsigned limit) const
    {
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(searchMappings+3, names, tableName);  // mappings+3 means we don't return the key columns (xpath, upper(keyPrefix), upper(key))
        const char *wuidTest;
        const char *fieldTest;
        if (descending)
        {
            wuidTest = ">";
            fieldTest = wuid ? "=" : "<";
        }
        else
        {
            wuidTest = "<";
            fieldTest = wuid ? "=" : ">";
        }
        VStringBuffer selectQuery("select %s from %s where xpath='@totalThorTime' and fieldPrefix='%*s' and fieldValue %s '%s'", names.str()+1, tableName.str(), CASS_SEARCH_PREFIX_SIZE, "", fieldTest, threshold);
        if (wuid)
            selectQuery.appendf(" and wuid %s '%s'", wuidTest, wuid);
        if (descending)
            selectQuery.append(" ORDER BY fieldValue DESC, WUID ASC");
        else
            selectQuery.append(" ORDER BY fieldValue ASC, WUID DESC");
        if (limit)
            selectQuery.appendf(" LIMIT %u", limit);
        selectQuery.append(';');
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch rows from the file search table

    const CassResult *fetchDataForFileRead(const char *name, const IArrayOf<IPostFilter> &wuidFilters, unsigned limit) const
    {
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(filesReadSearchMappings+1, names, tableName);  // mappings+3 means we don't return the key column (name)
        VStringBuffer selectQuery("select %s from %s where name='%s'", names.str()+1, tableName.str(), name);
        ForEachItemIn(idx, wuidFilters)
        {
            const IPostFilter &wuidFilter = wuidFilters.item(idx);
            selectQuery.appendf(" and wuid %s '%s'", wuidFilter.queryField()==WUSFwuidhigh ? "<=" : ">=", wuidFilter.queryValue());
        }
        if (limit)
            selectQuery.appendf(" LIMIT %u", limit);
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Fetch matching rows from the search table, for a single wuid

    const CassResult *fetchDataForKeyAndWuid(const char *xpath, const char *key, const char *wuid) const
    {
        assertex(key);
        StringBuffer names;
        StringBuffer tableName;
        StringBuffer ucKey(key);
        ucKey.toUpperCase();
        getFieldNames(searchMappings+4, names, tableName);  // mappings+4 means we don't return the key columns (xpath, upper(keyPrefix), upper(key), and wuid)
        VStringBuffer selectQuery("select %s from %s where xpath='%s' and fieldPrefix='%.*s' and fieldValue ='%s' and wuid='%s';", names.str()+1, tableName.str(), xpath, CASS_SEARCH_PREFIX_SIZE, ucKey.str(), ucKey.str(), wuid); // MORE - should consider using prepared/bind for this - is it faster?
        if (traceLevel >= 2)
            DBGLOG("%s", selectQuery.str());
        CassandraStatement statement(cass_statement_new(selectQuery.str(), 0));
        return executeQuery(session, statement);
    }

    // Delete matching rows from a child table

    virtual void deleteChildByWuid(const CassandraXmlMapping *mappings, const char *wuid, CassBatch *batch) const
    {
        StringBuffer names;
        StringBuffer tableName;
        getFieldNames(mappings, names, tableName);
        VStringBuffer insertQuery("DELETE from %s where partition=? and wuid=?;", tableName.str());
        Owned<CassandraPrepared> prepared = prepareStatement(insertQuery);
        CassandraStatement update(cass_prepared_bind(*prepared));
        check(cass_statement_bind_int32(update, 0, rtlHash32VStr(wuid, 0) % NUM_PARTITIONS));
        check(cass_statement_bind_string(update, 1, wuid));
        check(cass_batch_add_statement(batch, update));
    }

    unsigned retireCache()
    {
        CriticalBlock b(cacheCrit); // Is this too coarse-grained?
        unsigned expires = CASS_WU_QUERY_EXPIRES;
        unsigned now = msTick();
        ICopyArrayOf<CCassandraWuUQueryCacheEntry> goers;
        HashIterator iter(cacheIdMap);
        ForEach(iter)
        {
            CCassandraWuUQueryCacheEntry *entry = cacheIdMap.mapToValue(&iter.query());
            unsigned age = now - entry->queryLastAccess();
            int ttl = CASS_WU_QUERY_EXPIRES-age;
            if (ttl<= 0)
                goers.append(*entry);
            else if (ttl< expires)
                expires = ttl;
        }
        ForEachItemIn(idx, goers)
        {
            DBGLOG("Expiring cache entry %p", &goers.item(idx));
            cacheIdMap.remove(goers.item(idx).queryHint());
        }
        return expires;
    }

    class CacheRetirer : public Thread
    {
    public:
        CacheRetirer(CCasssandraWorkUnitFactory &_parent) : Thread("WorkunitListCacheRetirer"), parent(_parent)
        {
            stopping = false;
        }
        virtual int run()
        {
            while (!stopping)
            {
                unsigned delay = parent.retireCache();
                sem.wait(delay);
            }
            return 0;
        }
        void stop()
        {
            stopping = true;
            sem.signal();
        }
    private:
        Semaphore sem;
        CCasssandraWorkUnitFactory &parent;
        bool stopping;
    } cacheRetirer;

    unsigned randomizeSuffix;
    unsigned traceLevel;
    unsigned randState;
    CassandraCluster cluster;
    CassandraSession session;
    mutable CriticalSection cacheCrit;  // protects both of the caches below... we could separate
    mutable MapStringToMyClass<CassandraPrepared> preparedCache;
    mutable MapXToMyClass<__uint64, __uint64, CCassandraWuUQueryCacheEntry> cacheIdMap;
};


} // namespace

extern "C" EXPORT IWorkUnitFactory *createWorkUnitFactory(const IPropertyTree *props)
{
    return new cassandraembed::CCasssandraWorkUnitFactory(props);
}

