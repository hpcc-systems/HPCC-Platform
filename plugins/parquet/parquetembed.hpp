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

#ifndef _PARQUETEMBED_INCL
#define _PARQUETEMBED_INCL

#ifdef PARQUETEMBED_PLUGIN_EXPORTS
#define PARQUETEMBED_PLUGIN_API DECL_EXPORT
#else
#define PARQUETEMBED_PLUGIN_API DECL_IMPORT
#endif

#define RAPIDJSON_HAS_STDSTRING 1

#include "arrow/api.h"
#include "arrow/dataset/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/file.h"
#include "arrow/util/logging.h"
#include "arrow/ipc/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"

// Platform includes
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"
#include "tokenserialization.hpp"
#include "rtlfield.hpp"
#include "roxiemem.hpp"

#include <iostream>
#include <mutex>

namespace parquetembed
{
extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
extern void failx(const char *msg, ...) __attribute__((noreturn)) __attribute__((format(printf, 1, 2)));
extern void fail(const char *msg) __attribute__((noreturn));

#define reportIfFailure(st)                                                \
    if (!st.ok())                                                          \
    {                                                                      \
        failx("%s: %s.", st.CodeAsString().c_str(), st.message().c_str()); \
    }

static void typeError(const char *expected, const char *fieldname)
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
    const RtlFieldInfo *const *fields = record->queryFields();
    assertex(fields);
    while (*fields++)
        count++;
    return count;
}

static void handleDeserializeOutcome(DeserializationResult resultcode, const char *targetype, const char *culpritvalue)
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

enum PathNodeType {CPNTScalar, CPNTDataset, CPNTSet};

struct PathTracker
{
    const char *nodeName;
    PathNodeType nodeType;
    const arrow::Array *structPtr;
    unsigned int childCount = 0;
    unsigned int childrenProcessed = 0;

    PathTracker(const char *_nodeName, const arrow::Array *_struct, PathNodeType _nodeType)
        : nodeName(_nodeName), nodeType(_nodeType), structPtr(_struct) {}

    bool finishedChildren() { return childrenProcessed < childCount; }
};

enum ParquetArrayType
{
    NullType,
    BoolType,
    IntType,
    UIntType,
    DateType,
    TimestampType,
    TimeType,
    DurationType,
    StringType,
    LargeStringType,
    BinaryType,
    LargeBinaryType,
    DecimalType,
    ListType,
    StructType,
    RealType
};

class ParquetArrayVisitor : public arrow::ArrayVisitor
{
public:
    arrow::Status Visit(const arrow::NullArray &array)
    {
        type = NullType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::BooleanArray &array)
    {
        bool_arr = &array;
        type = BoolType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int8Array &array)
    {
        int8_arr = &array;
        type = IntType;
        size = 8;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int16Array &array)
    {
        int16_arr = &array;
        type = IntType;
        size = 16;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int32Array &array)
    {
        int32_arr = &array;
        type = IntType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int64Array &array)
    {
        int64_arr = &array;
        type = IntType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt8Array &array)
    {
        uint8_arr = &array;
        type = UIntType;
        size = 8;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt16Array &array)
    {
        uint16_arr = &array;
        type = UIntType;
        size = 16;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt32Array &array)
    {
        uint32_arr = &array;
        type = UIntType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt64Array &array)
    {
        uint64_arr = &array;
        type = UIntType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Date32Array &array)
    {
        date32_arr = &array;
        type = DateType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Date64Array &array)
    {
        date64_arr = &array;
        type = DateType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::TimestampArray &array)
    {
        timestamp_arr = &array;
        type = TimestampType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Time32Array &array)
    {
        time32_arr = &array;
        type = TimeType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Time64Array &array)
    {
        time64_arr = &array;
        type = TimeType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::DurationArray &array)
    {
        duration_arr = &array;
        type = DurationType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::HalfFloatArray &array)
    {
        half_float_arr = &array;
        type = RealType;
        size = 2;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::FloatArray &array)
    {
        float_arr = &array;
        type = RealType;
        size = 4;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::DoubleArray &array)
    {
        double_arr = &array;
        type = RealType;
        size = 8;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::StringArray &array)
    {
        string_arr = &array;
        type = StringType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::LargeStringArray &array)
    {
        large_string_arr = &array;
        type = LargeStringType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::BinaryArray &array)
    {
        bin_arr = &array;
        type = BinaryType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::LargeBinaryArray &array)
    {
        large_bin_arr = &array;
        type = LargeBinaryType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Decimal128Array &array)
    {
        dec_arr = &array;
        type = DecimalType;
        size = 128;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Decimal256Array &array)
    {
        large_dec_arr = &array;
        type = DecimalType;
        size = 256;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::ListArray &array)
    {
        list_arr = &array;
        type = ListType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::StructArray &array)
    {
        struct_arr = &array;
        type = StructType;
        return arrow::Status::OK();
    }

    ParquetArrayType type = NullType;
    int size = 0;
    const arrow::BooleanArray *bool_arr = nullptr;
    const arrow::Int8Array *int8_arr = nullptr;
    const arrow::Int16Array *int16_arr = nullptr;
    const arrow::Int32Array *int32_arr = nullptr;
    const arrow::Int64Array *int64_arr = nullptr;
    const arrow::UInt8Array *uint8_arr = nullptr;
    const arrow::UInt16Array *uint16_arr = nullptr;
    const arrow::UInt32Array *uint32_arr = nullptr;
    const arrow::UInt64Array *uint64_arr = nullptr;
    const arrow::Date32Array *date32_arr = nullptr;
    const arrow::Date64Array *date64_arr = nullptr;
    const arrow::TimestampArray *timestamp_arr = nullptr;
    const arrow::Time32Array *time32_arr = nullptr;
    const arrow::Time64Array *time64_arr = nullptr;
    const arrow::DurationArray *duration_arr = nullptr;
    const arrow::HalfFloatArray *half_float_arr = nullptr;
    const arrow::FloatArray *float_arr = nullptr;
    const arrow::DoubleArray *double_arr = nullptr;
    const arrow::StringArray *string_arr = nullptr;
    const arrow::LargeStringArray *large_string_arr = nullptr;
    const arrow::BinaryArray *bin_arr = nullptr;
    const arrow::LargeBinaryArray *large_bin_arr = nullptr;
    const arrow::Decimal128Array *dec_arr = nullptr;
    const arrow::Decimal256Array *large_dec_arr = nullptr;
    const arrow::ListArray *list_arr = nullptr;
    const arrow::StructArray *struct_arr = nullptr;
};

const rapidjson::Value kNullJsonSingleton = rapidjson::Value();

class DocValuesIterator
{
public:
    /// \param rows vector of rows
    /// \param path field names to enter
    /// \param array_levels number of arrays to enter
    DocValuesIterator(const std::vector<rapidjson::Document> &_rows,
                        std::vector<std::string> &&_path, int64_t _array_levels)
        : rows(_rows), path(std::move(_path)), array_levels(_array_levels) {}

    ~DocValuesIterator() = default;

    const rapidjson::Value *NextArrayOrRow(const rapidjson::Value *value, size_t *path_i,
                                            int64_t *arr_i)
    {
        while (array_stack.size() > 0)
        {
            ArrayPosition &pos = array_stack.back();
            // Try to get next position in Array
            if (pos.index + 1 < pos.array_node->Size())
            {
                ++pos.index;
                value = &(*pos.array_node)[pos.index];
                *path_i = pos.path_index;
                *arr_i = array_stack.size();
                return value;
            }
            else
            {
                array_stack.pop_back();
            }
        }
        ++row_i;
        if (row_i < rows.size())
        {
            value = static_cast<const rapidjson::Value *>(&rows[row_i]);
        }
        else
        {
            value = nullptr;
        }
        *path_i = 0;
        *arr_i = 0;
        return value;
    }

    arrow::Result<const rapidjson::Value *> Next()
    {
        const rapidjson::Value *value = nullptr;
        size_t path_i;
        int64_t arr_i;
        // Can either start at document or at last array level
        if (array_stack.size() > 0)
        {
            auto &pos = array_stack.back();
            value = pos.array_node;
            path_i = pos.path_index;
            arr_i = array_stack.size() - 1;
        }

        value = NextArrayOrRow(value, &path_i, &arr_i);

        // Traverse to desired level (with possible backtracking as needed)
        while (path_i < path.size() || arr_i < array_levels)
        {
            if (value == nullptr)
            {
                return value;
            }
            else if (value->IsArray() && value->Size() > 0)
            {
                ArrayPosition pos;
                pos.array_node = value;
                pos.path_index = path_i;
                pos.index = 0;
                array_stack.push_back(pos);

                value = &(*value)[0];
                ++arr_i;
            }
            else if (value->IsArray())
            {
                // Empty array means we need to backtrack and go to next array or row
                value = NextArrayOrRow(value, &path_i, &arr_i);
            }
            else if (value->HasMember(path[path_i]))
            {
                value = &(*value)[path[path_i]];
                ++path_i;
            }
            else
            {
                return &kNullJsonSingleton;
            }
        }

        // Return value
        return value;
    }

private:
    const std::vector<rapidjson::Document> &rows;
    std::vector<std::string> path;
    int64_t array_levels;
    size_t row_i = -1; // index of current row

    // Info about array position for one array level in array stack
    struct ArrayPosition
    {
        const rapidjson::Value *array_node;
        int64_t path_index;
        rapidjson::SizeType index;
    };
    std::vector<ArrayPosition> array_stack;
};

class JsonValueConverter
{
public:
    explicit JsonValueConverter(const std::vector<rapidjson::Document> &rows)
        : rows_(rows) {}

    JsonValueConverter(const std::vector<rapidjson::Document> &rows, const std::vector<std::string> &root_path, int64_t array_levels)
        : rows_(rows), root_path_(root_path), array_levels_(array_levels) {}

    ~JsonValueConverter() = default;

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field &field, arrow::ArrayBuilder *builder)
    {
        return Convert(field, field.name(), builder);
    }

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field &field, const std::string &field_name, arrow::ArrayBuilder *builder)
    {
        field_name_ = field_name;
        builder_ = builder;
        ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field.type().get(), this));
        return arrow::Status::OK();
    }

    // Default implementation
    arrow::Status Visit(const arrow::DataType &type)
    {
        return arrow::Status::NotImplemented("Can not convert json value to Arrow array of type ", type.ToString());
    }

    arrow::Status Visit(const arrow::LargeBinaryType &type)
    {
        arrow::LargeBinaryBuilder *builder = static_cast<arrow::LargeBinaryBuilder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetString(), value->GetStringLength()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int64Type &type)
    {
        arrow::Int64Builder *builder = static_cast<arrow::Int64Builder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                if (value->IsInt())
                {
                    ARROW_RETURN_NOT_OK(builder->Append(value->GetInt()));
                }
                else if (value->IsInt64())
                {
                    ARROW_RETURN_NOT_OK(builder->Append(value->GetInt64()));
                }
                else
                {
                    return arrow::Status::Invalid("Value is not an integer");
                }
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int32Type &type)
    {
        arrow::Int32Builder *builder = static_cast<arrow::Int32Builder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetInt()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt64Type &type)
    {
        arrow::Int64Builder *builder = static_cast<arrow::Int64Builder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                if (value->IsUint())
                {
                    ARROW_RETURN_NOT_OK(builder->Append(value->GetUint()));
                }
                else if (value->IsUint64())
                {
                    ARROW_RETURN_NOT_OK(builder->Append(value->GetUint64()));
                }
                else
                {
                    return arrow::Status::Invalid("Value is not an integer");
                }
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt32Type &type)
    {
        arrow::UInt32Builder *builder = static_cast<arrow::UInt32Builder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetUint()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::FloatType &type)
    {
        arrow::FloatBuilder *builder = static_cast<arrow::FloatBuilder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetFloat()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::DoubleType &type)
    {
        arrow::DoubleBuilder *builder = static_cast<arrow::DoubleBuilder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetDouble()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StringType &type)
    {
        arrow::StringBuilder *builder = static_cast<arrow::StringBuilder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetString(), value->GetStringLength()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::BooleanType &type)
    {
        arrow::BooleanBuilder *builder = static_cast<arrow::BooleanBuilder *>(builder_);
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            if (value->IsNull())
            {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetBool()));
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StructType &type)
    {
        arrow::StructBuilder *builder = static_cast<arrow::StructBuilder *>(builder_);

        std::vector<std::string> child_path(root_path_);
        if (field_name_.size() > 0)
        {
            child_path.push_back(field_name_);
        }
        auto child_converter = JsonValueConverter(rows_, child_path, array_levels_);

        for (int i = 0; i < type.num_fields(); ++i)
        {
            std::shared_ptr<arrow::Field> child_field = type.field(i);
            std::shared_ptr<arrow::ArrayBuilder> child_builder = builder->child_builder(i);

            ARROW_RETURN_NOT_OK(child_converter.Convert(*child_field.get(), child_builder.get()));
        }

        // Make null bitunordered_map
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
        }

        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::ListType &type)
    {
        arrow::ListBuilder *builder = static_cast<arrow::ListBuilder *>(builder_);

        // Values and offsets needs to be interleaved in ListBuilder, so first collect the
        // values
        std::unique_ptr<arrow::ArrayBuilder> tmp_value_builder;
        ARROW_ASSIGN_OR_RAISE(tmp_value_builder, arrow::MakeBuilder(builder->value_builder()->type()));
        std::vector<std::string> child_path(root_path_);
        child_path.push_back(field_name_);
        auto child_converter = JsonValueConverter(rows_, child_path, array_levels_ + 1);
        ARROW_RETURN_NOT_OK(child_converter.Convert(*type.value_field().get(), "", tmp_value_builder.get()));

        std::shared_ptr<arrow::Array> values_array;
        ARROW_RETURN_NOT_OK(tmp_value_builder->Finish(&values_array));
        std::shared_ptr<arrow::ArrayData> values_data = values_array->data();

        arrow::ArrayBuilder *value_builder = builder->value_builder();
        int64_t offset = 0;
        for (const auto &maybe_value : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
            ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
            if (!value->IsNull() && value->Size() > 0)
            {
                ARROW_RETURN_NOT_OK(value_builder->AppendArraySlice(*values_data.get(), offset, value->Size()));
                offset += value->Size();
            }
        }

        return arrow::Status::OK();
    }

private:
    std::string field_name_;
    arrow::ArrayBuilder *builder_;
    const std::vector<rapidjson::Document> &rows_;
    std::vector<std::string> root_path_;
    int64_t array_levels_ = 0;

    /// Return a flattened iterator over values at nested location
    arrow::Iterator<const rapidjson::Value *> FieldValues()
    {
        std::vector<std::string> path(root_path_);
        if (field_name_.size() > 0)
        {
            path.push_back(field_name_);
        }

        auto iter = DocValuesIterator(rows_, std::move(path), array_levels_);
        auto fn = [iter]() mutable -> arrow::Result<const rapidjson::Value *>
        { return iter.Next(); };

        return arrow::MakeFunctionIterator(fn);
    }
};

/**
 * @brief ParquetHelper holds the inputs from the user, the file stream objects, function for setting the schema, and functions
 * for opening parquet files.
 */
class ParquetHelper
{
public:
    ParquetHelper(const char *option, const char *location, const char *destination, int rowsize, int _batchSize, const IThorActivityContext *_activityCtx);
    ~ParquetHelper();
    std::shared_ptr<arrow::Schema> getSchema();
    arrow::Status openWriteFile();
    arrow::Status openReadFile();
    arrow::Status processReadFile();
    arrow::Status writePartition(std::shared_ptr<arrow::Table> table);
    parquet::arrow::FileWriter *queryWriter();
    void chunkTable(std::shared_ptr<arrow::Table> &table);
    rapidjson::Value *queryCurrentRow();
    void updateRow();
    std::vector<rapidjson::Document> &queryRecordBatch();
    bool partSetting();
    __int64 getMaxRowSize();
    char queryPartOptions();
    bool shouldRead();
    __int64 &getRowsProcessed();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> convertToRecordBatch(const std::vector<rapidjson::Document> &rows, std::shared_ptr<arrow::Schema> schema);
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> &next();
    std::shared_ptr<parquet::arrow::RowGroupReader> queryCurrentTable(__int64 currTable);
    arrow::Result<std::shared_ptr<arrow::Table>> queryRows();
    __int64 queryRowsCount();
    std::shared_ptr<arrow::NestedType> makeChildRecord(const RtlFieldInfo *field);
    arrow::Status fieldToNode(const std::string &name, const RtlFieldInfo *field, std::vector<std::shared_ptr<arrow::Field>> &arrow_fields);
    arrow::Status fieldsToSchema(const RtlTypeInfo *typeInfo);
    void beginSet();
    void beginRow();
    void endRow(const char *name);

private:
    __int64 currentRow = 0;
    __int64 rowSize = 0;                                             // The maximum size of each parquet row group.
    __int64 tablesProcessed = 0;                                      // Current RowGroup that has been read from the input file.
    __int64 rowsProcessed = 0;                                        // Current Row that has been read from the RowGroup
    __int64 startRowGroup = 0;                                      // The beginning RowGroup that is read by a worker
    __int64 tableCount = 0;                                           // The number of RowGroups to be read by the worker from the file that was opened for reading.
    __int64 rowsCount = 0;                                            // The number of result rows in a given RowGroup read from the parquet file.
    size_t batchSize = 0;                                            // batchSize for converting Parquet Columns to ECL rows. It is more efficient to break the data into small batches for converting to rows than to convert all at once.
    bool partition;                                               // Boolean variable to track whether we are writing partitioned files or not.
    std::string partOption;                                         // Read, r, Write, w, option for specifying parquet operation.
    std::string location;                                         // Location to read parquet file from.
    std::string destination;                                      // Destination to write parquet file to.
    const IThorActivityContext *activityCtx;                      // Additional local context information
    std::shared_ptr<arrow::Schema> schema = nullptr;              // Schema object that holds the schema of the file for reading and writing
    std::unique_ptr<parquet::arrow::FileWriter> writer = nullptr; // FileWriter for writing to parquet files.
    std::vector<rapidjson::Document> parquetDoc;                 // Document vector for converting rows to columns for writing to parquet files.
    std::vector<rapidjson::Value> rowStack;                      // Stack for keeping track of the context when building a nested row.
    std::shared_ptr<arrow::dataset::Scanner> scanner = nullptr;   // Scanner for reading through partitioned files. PARTITION
    arrow::dataset::FileSystemDatasetWriteOptions writeOptions;        // Write options for writing partitioned files. PARTITION
    std::shared_ptr<arrow::RecordBatchReader> rbatchReader = nullptr;
    arrow::RecordBatchReader::RecordBatchReaderIterator rbatchItr;
    std::vector<__int64> fileTableCounts;
    std::vector<std::unique_ptr<parquet::arrow::FileReader>> parquetFileReaders;
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> parquetTable;
    arrow::MemoryPool *pool = nullptr;
};

/**
 * @brief Builds ECL Records from Parquet result rows.
 *
 */
class ParquetRowStream : public RtlCInterface, implements IRowStream
{
public:
    ParquetRowStream(IEngineRowAllocator *_resultAllocator, std::shared_ptr<ParquetHelper> _parquet);
    virtual ~ParquetRowStream() = default;

    RTLIMPLEMENT_IINTERFACE
    virtual const void *nextRow() override;
    virtual void stop() override;

private:
    Linked<IEngineRowAllocator> m_resultAllocator; //! Pointer to allocator used when building result rows.
    bool m_shouldRead = true;                             //! If true, we should continue trying to read more messages.
    __int64 m_currentRow = 0;                          //! Current result row.
    __int64 rowsCount;                             //! Number of result rows read from parquet file.
    std::shared_ptr<ParquetArrayVisitor> array_visitor = nullptr;
    std::shared_ptr<ParquetHelper> s_parquet = nullptr; //! Shared pointer to ParquetHelper class for the stream class.
};

/**
 * @brief Builds ECL records for ParquetRowStream.
 *
 */
class ParquetRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    ParquetRowBuilder(std::unordered_map<std::string, std::shared_ptr<arrow::Array>> *_result_rows, int64_t _currentRow, std::shared_ptr<ParquetArrayVisitor> *_array_visitor)
        : result_rows(_result_rows), currentRow(_currentRow), array_visitor(_array_visitor) {}

    virtual ~ParquetRowBuilder() = default;

    virtual bool getBooleanResult(const RtlFieldInfo *field) override;
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void *&result) override;
    virtual double getRealResult(const RtlFieldInfo *field) override;
    virtual __int64 getSignedResult(const RtlFieldInfo *field) override;
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field) override;
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char *&result) override;
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char *&result) override;
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar *&result) override;
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value) override;
    virtual void processBeginSet(const RtlFieldInfo *field, bool &isAll) override;
    virtual bool processNextSet(const RtlFieldInfo *field) override;
    virtual void processBeginDataset(const RtlFieldInfo *field) override;
    virtual void processBeginRow(const RtlFieldInfo *field) override;
    virtual bool processNextRow(const RtlFieldInfo *field) override;
    virtual void processEndSet(const RtlFieldInfo *field) override;
    virtual void processEndDataset(const RtlFieldInfo *field) override;
    virtual void processEndRow(const RtlFieldInfo *field) override;

protected:
    const std::shared_ptr<arrow::Array> &getChunk(std::shared_ptr<arrow::ChunkedArray> *column);
    std::string_view getCurrView(const RtlFieldInfo *field);
    __int64 getCurrIntValue(const RtlFieldInfo *field);
    double getCurrRealValue(const RtlFieldInfo *field);
    void nextField(const RtlFieldInfo *field);
    void nextFromStruct(const RtlFieldInfo *field);
    void xpathOrName(StringBuffer &outXPath, const RtlFieldInfo *field) const;
    int64_t currArrayIndex();

private:
    __int64 currentRow;
    TokenDeserializer tokenDeserializer;
    TokenSerializer tokenSerializer;
    StringBuffer serialized;
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> *result_rows;
    std::vector<PathTracker> m_pathStack;
    std::shared_ptr<ParquetArrayVisitor> *array_visitor;
};

/**
 * @brief Binds ECL records to parquet objects
 *
 */
class ParquetRecordBinder : public CInterfaceOf<IFieldProcessor>
{
public:
    ParquetRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, int _firstParam, std::shared_ptr<ParquetHelper> _parquet)
        : logctx(_logctx), typeInfo(_typeInfo), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam)
    {
        r_parquet = _parquet;
        partition = _parquet->partSetting();
    }

    virtual ~ParquetRecordBinder() = default;

    int numFields();
    void processRow(const byte *row);
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo *field);
    virtual void processBool(bool value, const RtlFieldInfo *field);
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo *field);
    virtual void processInt(__int64 value, const RtlFieldInfo *field);
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo *field);
    virtual void processReal(double value, const RtlFieldInfo *field);
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo *field);
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo *field)
    {
        UNSUPPORTED("UNSIGNED decimals");
    }

    virtual void processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo *field);
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo *field);
    virtual void processUtf8(unsigned chars, const char *value, const RtlFieldInfo *field);
    virtual bool processBeginSet(const RtlFieldInfo *field, unsigned numElements, bool isAll, const byte *data)
    {
        r_parquet->beginSet();
        return true;
    }
    virtual bool processBeginDataset(const RtlFieldInfo *field, unsigned rowsCount)
    {
        UNSUPPORTED("DATASET");
        return false;
    }
    virtual bool processBeginRow(const RtlFieldInfo *field)
    {
        // There is a better way to do this than creating a stack and having to iterate back through to
        // copy over the members of the rapidjson value.
        // TO DO
        // Create a json string of all the fields which will be much more performant.
        r_parquet->beginRow();
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo *field)
    {
        r_parquet->endRow(field->name);
    }
    virtual void processEndDataset(const RtlFieldInfo *field)
    {
        UNSUPPORTED("DATASET");
    }
    virtual void processEndRow(const RtlFieldInfo *field)
    {
        r_parquet->endRow(field->name);
    }

protected:
    inline unsigned checkNextParam(const RtlFieldInfo *field);

    const RtlTypeInfo *typeInfo = nullptr;
    const IContextLogger &logctx;
    int firstParam;
    RtlFieldStrInfo dummyField;
    int thisParam;
    TokenSerializer m_tokenSerializer;

    std::shared_ptr<ParquetHelper> r_parquet;
    bool partition; //! Local copy of a boolean so we can know if we are writing partitioned files or not.
};

/**
 * @brief Binds an ECL dataset to a vector of parquet objects.
 *
 */
class ParquetDatasetBinder : public ParquetRecordBinder
{
public:
    /**
     * @brief Construct a new ParquetDataset Binder object
     *
     * @param _logctx logger for building the dataset.
     * @param _input Stream of input of dataset.
     * @param _typeInfo Field type info.
     * @param _query Holds the builder object for creating the documents.
     * @param _firstParam Index of the first param.
     */
    ParquetDatasetBinder(const IContextLogger &_logctx, IRowStream *_input, const RtlTypeInfo *_typeInfo, std::shared_ptr<ParquetHelper> _parquet, int _firstParam)
        : input(_input), ParquetRecordBinder(_logctx, _typeInfo, _firstParam, _parquet)
    {
        d_parquet = _parquet;
        reportIfFailure(d_parquet->fieldsToSchema(_typeInfo));
    }
    virtual ~ParquetDatasetBinder() = default;
    void getFieldTypes(const RtlTypeInfo *typeInfo);
    bool bindNext();
    void writeRecordBatch();
    void executeAll();

protected:
    Owned<IRowStream> input;
    std::shared_ptr<ParquetHelper> d_parquet; //! Helper object for keeping track of read and write options, schema, and file names.
};

/**
 * @brief Main interface for the engine to interact with the plugin. The get functions return results to the engine and the Rowstream and
 *
 */
class ParquetEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    ParquetEmbedFunctionContext(const IContextLogger &_logctx, const IThorActivityContext *activityCtx, const char *options, unsigned _flags);
    virtual ~ParquetEmbedFunctionContext() = default;
    virtual bool getBooleanResult();
    virtual void getDataResult(size32_t &len, void *&result);
    virtual double getRealResult();
    virtual __int64 getSignedResult();
    virtual unsigned __int64 getUnsignedResult();
    virtual void getStringResult(size32_t &chars, char *&result);
    virtual void getUTF8Result(size32_t &chars, char *&result);
    virtual void getUnicodeResult(size32_t &chars, UChar *&result);
    virtual void getDecimalResult(Decimal &value);
    virtual void getSetResult(bool &__isAllResult, size32_t &__resultBytes, void *&__result, int elemType, size32_t elemSize)
    {
        UNSUPPORTED("SET results");
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator *_resultAllocator);
    virtual byte *getRowResult(IEngineRowAllocator *_resultAllocator);
    virtual size32_t getTransformResult(ARowBuilder &rowBuilder);
    virtual void bindRowParam(const char *name, IOutputMetaData &metaVal, const byte *val) override;
    virtual void bindDatasetParam(const char *name, IOutputMetaData &metaVal, IRowStream *val);
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
    unsigned checkNextParam(const char *name);
    const IContextLogger &logctx;
    Owned<IPropertyTreeIterator> m_resultrow;

    Owned<ParquetDatasetBinder> m_oInputStream; //! Input Stream used for building a dataset.

    TokenDeserializer tokenDeserializer;
    TokenSerializer m_tokenSerializer;
    unsigned m_nextParam = 0;   //! Index of the next parameter to process.
    unsigned m_numParams = 0;   //! Number of parameters in the function definition.
    unsigned m_scriptFlags;     //! Count of flags raised by embedded script.

    std::shared_ptr<ParquetHelper> m_parquet; //! Helper object for keeping track of read and write options, schema, and file names.
};
}
#endif
