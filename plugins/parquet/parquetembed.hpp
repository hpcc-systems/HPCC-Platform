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
#include "rtlfield.hpp"
#include "roxiemem.hpp"

#include <iostream>
#include <mutex>

namespace parquetembed
{
extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
extern void failx(const char *msg, ...) __attribute__((noreturn)) __attribute__((format(printf, 1, 2)));
extern void fail(const char *msg) __attribute__((noreturn));

#define PARQUET_FILE_TYPE_NAME "parquet"

#define reportIfFailure(st)                                                \
    if (!st.ok())                                                          \
    {                                                                      \
        failx("%s: %s.", st.CodeAsString().c_str(), st.message().c_str()); \
    }

static void typeError(const char *expected, const char *fieldname)
{
    VStringBuffer msg("parquetembed: type mismatch - %s expected", expected);
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

enum PathNodeType {CPNTScalar, CPNTDataset, CPNTSet};

/**
 * @brief Keep track of nested structures when binding rows to parquet.
 */
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

/**
 * @brief A Visitor type class that implements every arrow type and gets a pointer to the visited array in the correct type.
 * The size and type of the array are stored to read the correct array when returning values.
*/
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
        boolArr = &array;
        type = BoolType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int8Array &array)
    {
        int8Arr = &array;
        type = IntType;
        size = 8;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int16Array &array)
    {
        int16Arr = &array;
        type = IntType;
        size = 16;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int32Array &array)
    {
        int32Arr = &array;
        type = IntType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Int64Array &array)
    {
        int64Arr = &array;
        type = IntType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt8Array &array)
    {
        uint8Arr = &array;
        type = UIntType;
        size = 8;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt16Array &array)
    {
        uint16Arr = &array;
        type = UIntType;
        size = 16;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt32Array &array)
    {
        uint32Arr = &array;
        type = UIntType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::UInt64Array &array)
    {
        uint64Arr = &array;
        type = UIntType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Date32Array &array)
    {
        date32Arr = &array;
        type = DateType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Date64Array &array)
    {
        date64Arr = &array;
        type = DateType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::TimestampArray &array)
    {
        timestampArr = &array;
        type = TimestampType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Time32Array &array)
    {
        time32Arr = &array;
        type = TimeType;
        size = 32;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Time64Array &array)
    {
        time64Arr = &array;
        type = TimeType;
        size = 64;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::DurationArray &array)
    {
        durationArr = &array;
        type = DurationType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::HalfFloatArray &array)
    {
        halfFloatArr = &array;
        type = RealType;
        size = 2;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::FloatArray &array)
    {
        floatArr = &array;
        type = RealType;
        size = 4;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::DoubleArray &array)
    {
        doubleArr = &array;
        type = RealType;
        size = 8;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::StringArray &array)
    {
        stringArr = &array;
        type = StringType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::LargeStringArray &array)
    {
        largeStringArr = &array;
        type = LargeStringType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::BinaryArray &array)
    {
        binArr = &array;
        type = BinaryType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::LargeBinaryArray &array)
    {
        largeBinArr = &array;
        type = LargeBinaryType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Decimal128Array &array)
    {
        decArr = &array;
        type = DecimalType;
        size = 128;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::Decimal256Array &array)
    {
        largeDecArr = &array;
        type = DecimalType;
        size = 256;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::ListArray &array)
    {
        listArr = &array;
        type = ListType;
        return arrow::Status::OK();
    }
    arrow::Status Visit(const arrow::StructArray &array)
    {
        structArr = &array;
        type = StructType;
        return arrow::Status::OK();
    }

    ParquetArrayType type = NullType;               // Type of the Array that was read.
    int size = 0;                                   // For Signed, Unsigned, and Real types size differentiates between the different array types.
    const arrow::BooleanArray *boolArr = nullptr;   // A pointer to the tables column that is stored in memory for the correct value type.
    const arrow::Int8Array *int8Arr = nullptr;
    const arrow::Int16Array *int16Arr = nullptr;
    const arrow::Int32Array *int32Arr = nullptr;
    const arrow::Int64Array *int64Arr = nullptr;
    const arrow::UInt8Array *uint8Arr = nullptr;
    const arrow::UInt16Array *uint16Arr = nullptr;
    const arrow::UInt32Array *uint32Arr = nullptr;
    const arrow::UInt64Array *uint64Arr = nullptr;
    const arrow::Date32Array *date32Arr = nullptr;
    const arrow::Date64Array *date64Arr = nullptr;
    const arrow::TimestampArray *timestampArr = nullptr;
    const arrow::Time32Array *time32Arr = nullptr;
    const arrow::Time64Array *time64Arr = nullptr;
    const arrow::DurationArray *durationArr = nullptr;
    const arrow::HalfFloatArray *halfFloatArr = nullptr;
    const arrow::FloatArray *floatArr = nullptr;
    const arrow::DoubleArray *doubleArr = nullptr;
    const arrow::StringArray *stringArr = nullptr;
    const arrow::LargeStringArray *largeStringArr = nullptr;
    const arrow::BinaryArray *binArr = nullptr;
    const arrow::LargeBinaryArray *largeBinArr = nullptr;
    const arrow::Decimal128Array *decArr = nullptr;
    const arrow::Decimal256Array *largeDecArr = nullptr;
    const arrow::ListArray *listArr = nullptr;
    const arrow::StructArray *structArr = nullptr;
};

const rapidjson::Value kNullJsonSingleton = rapidjson::Value();

class DocValuesIterator
{
public:
    DocValuesIterator(const std::vector<rapidjson::Document> &_rows,
                        std::vector<std::string> &&_path, int64_t _arrayLevels)
        : rows(_rows), path(std::move(_path)), arrayLevels(_arrayLevels) {}
    ~DocValuesIterator() = default;

    const rapidjson::Value *NextArrayOrRow(const rapidjson::Value *value, size_t *pathIdx, int64_t *arrIdx)
    {
        while (arrayStack.size() > 0)
        {
            ArrayPosition &pos = arrayStack.back();
            // Try to get next position in Array
            if (pos.index + 1 < pos.arrayNode->Size())
            {
                ++pos.index;
                value = &(*pos.arrayNode)[pos.index];
                *pathIdx = pos.pathIndex;
                *arrIdx = arrayStack.size();
                return value;
            }
            else
            {
                arrayStack.pop_back();
            }
        }
        ++rowIdx;
        if (rowIdx < rows.size())
        {
            value = static_cast<const rapidjson::Value *>(&rows[rowIdx]);
        }
        else
        {
            value = nullptr;
        }
        *pathIdx = 0;
        *arrIdx = 0;
        return value;
    }

    arrow::Result<const rapidjson::Value *> Next()
    {
        const rapidjson::Value *value = nullptr;
        size_t pathIdx;
        int64_t arrIdx;
        // Can either start at document or at last array level
        if (arrayStack.size() > 0)
        {
            auto &pos = arrayStack.back();
            value = pos.arrayNode;
            pathIdx = pos.pathIndex;
            arrIdx = arrayStack.size() - 1;
        }

        value = NextArrayOrRow(value, &pathIdx, &arrIdx);

        // Traverse to desired level (with possible backtracking as needed)
        while (pathIdx < path.size() || arrIdx < arrayLevels)
        {
            if (value == nullptr)
            {
                return value;
            }
            else if (value->IsArray() && value->Size() > 0)
            {
                ArrayPosition pos;
                pos.arrayNode = value;
                pos.pathIndex = pathIdx;
                pos.index = 0;
                arrayStack.push_back(pos);

                value = &(*value)[0];
                ++arrIdx;
            }
            else if (value->IsArray())
            {
                // Empty array means we need to backtrack and go to next array or row
                value = NextArrayOrRow(value, &pathIdx, &arrIdx);
            }
            else if (value->HasMember(path[pathIdx]))
            {
                value = &(*value)[path[pathIdx]];
                ++pathIdx;
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
    int64_t arrayLevels;
    size_t rowIdx = -1; // Index of current row

    // Info about array position for one array level in array stack
    struct ArrayPosition
    {
        const rapidjson::Value *arrayNode;
        int64_t pathIndex;
        rapidjson::SizeType index;
    };
    std::vector<ArrayPosition> arrayStack;
};

class JsonValueConverter
{
public:
    explicit JsonValueConverter(const std::vector<rapidjson::Document> &_rows)
        : rows(_rows) {}

    JsonValueConverter(const std::vector<rapidjson::Document> &_rows, const std::vector<std::string> &_rootPath, int64_t _arrayLevels)
        : rows(_rows), rootPath(_rootPath), arrayLevels(_arrayLevels) {}

    ~JsonValueConverter() = default;

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field &field, arrow::ArrayBuilder *builder)
    {
        return Convert(field, field.name(), builder);
    }

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field &field, const std::string &_fieldName, arrow::ArrayBuilder *builder)
    {
        fieldName = _fieldName;
        arrayBuilder = builder;
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
        arrow::LargeBinaryBuilder *builder = static_cast<arrow::LargeBinaryBuilder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::Int64Builder *builder = static_cast<arrow::Int64Builder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::Int32Builder *builder = static_cast<arrow::Int32Builder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::Int64Builder *builder = static_cast<arrow::Int64Builder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::UInt32Builder *builder = static_cast<arrow::UInt32Builder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::FloatBuilder *builder = static_cast<arrow::FloatBuilder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::DoubleBuilder *builder = static_cast<arrow::DoubleBuilder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::StringBuilder *builder = static_cast<arrow::StringBuilder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::BooleanBuilder *builder = static_cast<arrow::BooleanBuilder *>(arrayBuilder);
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
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
        arrow::StructBuilder *builder = static_cast<arrow::StructBuilder *>(arrayBuilder);

        std::vector<std::string> childPath(rootPath);
        if (fieldName.size() > 0)
        {
            childPath.push_back(fieldName);
        }
        auto child_converter = JsonValueConverter(rows, childPath, arrayLevels);

        for (int i = 0; i < type.num_fields(); ++i)
        {
            std::shared_ptr<arrow::Field> childField = type.field(i);
            std::shared_ptr<arrow::ArrayBuilder> childBuilder = builder->child_builder(i);

            ARROW_RETURN_NOT_OK(child_converter.Convert(*childField.get(), childBuilder.get()));
        }

        // Make null bitunordered_map
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
            ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
        }

        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::ListType &type)
    {
        arrow::ListBuilder *builder = static_cast<arrow::ListBuilder *>(arrayBuilder);

        // Values and offsets needs to be interleaved in ListBuilder, so first collect the values
        std::unique_ptr<arrow::ArrayBuilder> tmpValueBuilder;
        ARROW_ASSIGN_OR_RAISE(tmpValueBuilder, arrow::MakeBuilder(builder->value_builder()->type()));
        std::vector<std::string> childPath(rootPath);
        childPath.push_back(fieldName);
        auto child_converter = JsonValueConverter(rows, childPath, arrayLevels + 1);
        ARROW_RETURN_NOT_OK(child_converter.Convert(*type.value_field().get(), "", tmpValueBuilder.get()));

        std::shared_ptr<arrow::Array> valuesArray;
        ARROW_RETURN_NOT_OK(tmpValueBuilder->Finish(&valuesArray));
        std::shared_ptr<arrow::ArrayData> valuesData = valuesArray->data();

        arrow::ArrayBuilder *valueBuilder = builder->value_builder();
        int64_t offset = 0;
        for (const auto &maybeValue : FieldValues())
        {
            ARROW_ASSIGN_OR_RAISE(auto value, maybeValue);
            ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
            if (!value->IsNull() && value->Size() > 0)
            {
                ARROW_RETURN_NOT_OK(valueBuilder->AppendArraySlice(*valuesData.get(), offset, value->Size()));
                offset += value->Size();
            }
        }

        return arrow::Status::OK();
    }

private:
    std::string fieldName;
    arrow::ArrayBuilder *arrayBuilder = nullptr;
    const std::vector<rapidjson::Document> &rows;
    std::vector<std::string> rootPath;
    int64_t arrayLevels = 0;

    // Return a flattened iterator over values at nested location
    arrow::Iterator<const rapidjson::Value *> FieldValues()
    {
        std::vector<std::string> path(rootPath);
        if (fieldName.size() > 0)
        {
            path.push_back(fieldName);
        }

        auto iter = DocValuesIterator(rows, std::move(path), arrayLevels);
        auto fn = [iter]() mutable -> arrow::Result<const rapidjson::Value *>
        { return iter.Next(); };

        return arrow::MakeFunctionIterator(fn);
    }
};

using TableColumns = std::unordered_map<std::string, std::shared_ptr<arrow::Array>>;

/**
 * @brief Opens and reads Parquet files and partitioned datasets. The ParquetReader processes a file
 * based on the path passed in via location. processReadFile opens the file and sets the reader up to read rows.
 * The next function returns the index to read and the table to read from. shouldRead will return true as long as
 * the worker can read another row.
 */
class PARQUETEMBED_PLUGIN_API ParquetReader
{
public:
    ParquetReader(const char *option, const char *_location, int _maxRowCountInTable, const char *_partitionFields, const IThorActivityContext *_activityCtx);
    ~ParquetReader();
    arrow::Status openReadFile();
    arrow::Status processReadFile();
    void splitTable(std::shared_ptr<arrow::Table> &table);
    bool shouldRead();
    __int64 next(TableColumns *&nextTable);
    std::shared_ptr<parquet::arrow::RowGroupReader> queryCurrentTable(__int64 currTable);
    arrow::Result<std::shared_ptr<arrow::Table>> queryRows();

private:
    __int64 tablesProcessed = 0;                                       // The number of tables processed when reading parquet files.
    __int64 totalRowsProcessed = 0;                                    // Total number of rows processed of partitioned dataset. We cannot get the total number of chunks and they are variable sizes.
    __int64 totalRowCount = 0;                                         // Total number of rows in a partition dataset.
    __int64 startRow = 0;                                              // The starting row in a partitioned dataset.
    __int64 rowsProcessed = 0;                                         // Current Row that has been read from the RowGroup.
    __int64 startRowGroup = 0;                                         // The beginning RowGroup that is read by a worker.
    __int64 tableCount = 0;                                            // The number of RowGroups to be read by the worker from the file that was opened for reading.
    __int64 rowsCount = 0;                                             // The number of result rows in a given RowGroup read from the parquet file.
    size_t maxRowCountInTable = 0;                                     // Max table size set by user.
    std::string partOption;                                            // Begins with either read or write and ends with the partitioning type if there is one i.e. 'readhivepartition'.
    std::string location;                                              // Full path to location for reading parquet files. Can be a filename or directory.
    const IThorActivityContext *activityCtx = nullptr;                 // Context about the thor worker configuration.
    std::shared_ptr<arrow::dataset::Scanner> scanner = nullptr;        // Scanner for reading through partitioned files.
    std::shared_ptr<arrow::RecordBatchReader> rbatchReader = nullptr;                           // RecordBatchReader reads a dataset one record batch at a time. Must be kept alive for rbatchItr.
    arrow::RecordBatchReader::RecordBatchReaderIterator rbatchItr;                              // Iterator of RecordBatches when reading a partitioned dataset.
    std::vector<__int64> fileTableCounts;                                                       // Count of RowGroups in each open file to get the correct row group when reading specific parts of the file.
    std::vector<std::shared_ptr<parquet::arrow::FileReader>> parquetFileReaders;                // Vector of FileReaders that match the target file name. data0.parquet, data1.parquet, etc.
    TableColumns parquetTable;                                                                  // The current table being read broken up into columns. Unordered map where the left side is a string of the field name and the right side is an array of the values.
    std::vector<std::string> partitionFields;                                                   // The partitioning schema for reading Directory Partitioned files.
    arrow::MemoryPool *pool = nullptr;                                                          // Memory pool for reading parquet files.
};

/**
 * @brief Opens and writes to a Parquet file or writes RecordBatches to a partioned dataset. The ParquetWriter checks the destination
 * for existing data on construction and will fail if there are prexisiting files. If the overwrite option is set to true the data in
 * target directory or matching the file mask will be deleted and writing will continue. openWriteFile opens the write file or sets the
 * partitioning options. writeRecordBatch utilizes the open write streams and writes the data to the target location.
 */
class PARQUETEMBED_PLUGIN_API ParquetWriter
{
public:
    ParquetWriter(const char *option, const char *_destination, int _maxRowCountInBatch, bool _overwrite, arrow::Compression::type _compressionOption, const char *_partitionFields, const IThorActivityContext *_activityCtx);
    ~ParquetWriter();
    arrow::Status openWriteFile();
    arrow::Status writePartition(std::shared_ptr<arrow::Table> table);
    void writeRecordBatch();
    void writeRecordBatch(std::size_t newSize);
    rapidjson::Value *queryCurrentRow();
    void updateRow();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> convertToRecordBatch(const std::vector<rapidjson::Document> &rows, std::shared_ptr<arrow::Schema> schema);
    std::shared_ptr<arrow::NestedType> makeChildRecord(const RtlFieldInfo *field);
    arrow::Status fieldToNode(const std::string &name, const RtlFieldInfo *field, std::vector<std::shared_ptr<arrow::Field>> &arrowFields);
    arrow::Status fieldsToSchema(const RtlTypeInfo *typeInfo);
    void beginSet();
    void beginRow();
    void endRow(const char *name);
    void addMember(rapidjson::Value &key, rapidjson::Value &value);
    arrow::Status checkDirContents();
    __int64 getMaxRowSize() {return maxRowCountInBatch;}

private:
    __int64 currentRow = 0;
    __int64 maxRowCountInBatch = 0;                                    // The maximum size of each parquet row group.
    __int64 tablesProcessed = 0;                                       // Current RowGroup that has been read from the input file.
    std::string partOption;                                            // Begins with either read or write and ends with the partitioning type if there is one i.e. 'readhivepartition'.
    std::string destination;                                           // Full path to destination for writing parquet files. Can be a filename or directory.
    bool overwrite = false;                                            // Overwrite option specified by the user. If true the plugin will overwrite files that are already exisiting. Default is false.
    const IThorActivityContext *activityCtx = nullptr;                 // Context about the thor worker configuration.
    std::shared_ptr<arrow::Schema> schema = nullptr;                   // Arrow Schema holding the rtlTypeInfo when writing to parquet files.
    std::unique_ptr<parquet::arrow::FileWriter> writer = nullptr;      // FileWriter for writing to single parquet files.
    std::vector<rapidjson::Document> parquetDoc;                       // Document vector for converting rows to columns for writing to parquet files.
    std::vector<rapidjson::Value> rowStack;                            // Stack for keeping track of the context when building a nested row.
    arrow::dataset::FileSystemDatasetWriteOptions writeOptions;        // Write options for writing partitioned files.
    arrow::Compression::type compressionOption = arrow::Compression::type::UNCOMPRESSED;        // The compression type set by the user for compressing files on write.
    std::shared_ptr<arrow::dataset::Partitioning> partitionType = nullptr;                      // The partition type with the partitioning schema for creating a dataset.
    std::vector<std::string> partitionFields;                                                   // The partitioning schema.
    arrow::MemoryPool *pool = nullptr;                                                          // Memory pool for writing parquet files.
};

/**
 * @brief Builds ECL Records from Parquet result rows.
 *
 */
class ParquetRowStream : public RtlCInterface, implements IRowStream
{
public:
    ParquetRowStream(IEngineRowAllocator *_resultAllocator, std::shared_ptr<ParquetReader> _parquetReader)
        : resultAllocator(_resultAllocator), parquetReader(std::move(_parquetReader)) {}
    virtual ~ParquetRowStream() = default;

    RTLIMPLEMENT_IINTERFACE
    virtual const void *nextRow() override;
    virtual void stop() override;

private:
    Linked<IEngineRowAllocator> resultAllocator;                    // Pointer to allocator used when building result rows.
    bool shouldRead = true;                                         // If true, we should continue trying to read more messages.
    __int64 currentRow = 0;                                         // Current result row.
    std::shared_ptr<ParquetReader> parquetReader = nullptr;         // Parquet file reader.
};

/**
 * @brief Builds ECL records for ParquetRowStream.
 *
 */
class PARQUETEMBED_PLUGIN_API ParquetRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    ParquetRowBuilder(TableColumns *_resultRows, int64_t _currentRow)
        : resultRows(_resultRows), currentRow(_currentRow) {}
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
    std::string_view getCurrView(const RtlFieldInfo *field);
    __int64 getCurrIntValue(const RtlFieldInfo *field);
    double getCurrRealValue(const RtlFieldInfo *field);
    void nextField(const RtlFieldInfo *field);
    void nextFromStruct(const RtlFieldInfo *field);
    void xpathOrName(StringBuffer &outXPath, const RtlFieldInfo *field) const;
    int64_t currArrayIndex();

private:
    __int64 currentRow;                                                             // The index in the arrow Array to read the current value.
    StringBuffer serialized;                                                        // Output string from serialization.
    TableColumns *resultRows = nullptr;                                             // A pointer to the result rows map where the left side are the field names for the columns and the right is an array of values.
    std::vector<PathTracker> pathStack;                                             // PathTracker keeps track of nested data when reading sets.
    std::shared_ptr<ParquetArrayVisitor> arrayVisitor;                              // Visitor class for getting the correct type when reading a Parquet column.
};

/**
 * @brief Binds ECL records to parquet objects
 *
 */
class ParquetRecordBinder : public CInterfaceOf<IFieldProcessor>
{
public:
    ParquetRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, int _firstParam, std::shared_ptr<ParquetWriter> _parquetWriter)
        : logctx(_logctx), typeInfo(_typeInfo), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam), parquetWriter(std::move(_parquetWriter)) {}
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
        parquetWriter->beginSet();
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
        parquetWriter->beginRow();
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo *field)
    {
        parquetWriter->endRow(field->name);
    }
    virtual void processEndDataset(const RtlFieldInfo *field)
    {
        UNSUPPORTED("DATASET");
    }
    virtual void processEndRow(const RtlFieldInfo *field)
    {
        parquetWriter->endRow(field->name);
    }

protected:
    inline unsigned checkNextParam(const RtlFieldInfo *field);

    const RtlTypeInfo *typeInfo = nullptr;
    const IContextLogger &logctx;
    int firstParam;
    RtlFieldStrInfo dummyField;
    int thisParam;
    std::shared_ptr<ParquetWriter> parquetWriter;
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
     * @param _logctx Logger for building the dataset.
     * @param _input Stream of input of dataset.
     * @param _typeInfo Field type info.
     * @param _query Holds the builder object for creating the documents.
     * @param _firstParam Index of the first param.
     */
    ParquetDatasetBinder(const IContextLogger &_logctx, IRowStream *_input, const RtlTypeInfo *_typeInfo, std::shared_ptr<ParquetWriter> _parquetWriter, int _firstParam)
        : input(_input), parquetWriter(_parquetWriter), ParquetRecordBinder(_logctx, _typeInfo, _firstParam, _parquetWriter)
    {
        reportIfFailure(parquetWriter->fieldsToSchema(_typeInfo));
    }
    virtual ~ParquetDatasetBinder() = default;
    void getFieldTypes(const RtlTypeInfo *typeInfo);
    bool bindNext();
    void executeAll();

protected:
    Owned<IRowStream> input;
    std::shared_ptr<ParquetWriter> parquetWriter; // Parquet file writer.
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
    Owned<ParquetDatasetBinder> oInputStream;                   // Input Stream used for building a dataset.
    unsigned nextParam = 0;                                     // Index of the next parameter to process.
    unsigned numParams = 0;                                     // Number of parameters in the function definition.
    unsigned scriptFlags;                                       // Count of flags raised by embedded script.
    std::shared_ptr<ParquetReader> parquetReader = nullptr;     // Parquet File Reader
    std::shared_ptr<ParquetWriter> parquetWriter = nullptr;     // Parquet File Writer
};
}
#endif
