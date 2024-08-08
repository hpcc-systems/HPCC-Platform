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

#include "arrow/api.h"
#include "arrow/dataset/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/file.h"
#include "arrow/util/logging.h"
#include "arrow/ipc/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

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
 * @brief Keep track of nested arrays when binding Parquet columns to ECL rows.
 */
struct ParquetColumnTracker
{
    const RtlFieldInfo * field;
    PathNodeType nodeType;
    const arrow::Array *structPtr;
    unsigned int childCount = 0;
    unsigned int childrenProcessed = 0;

    ParquetColumnTracker(const RtlFieldInfo * _field, const arrow::Array *_struct, PathNodeType _nodeType)
        : field(_field), nodeType(_nodeType), structPtr(_struct) {}

    bool finishedChildren() { return childrenProcessed < childCount; }
};

/**
 * @brief Keep track of nested child builders when converting ECL rows to Parquet columns.
 */
struct ArrayBuilderTracker
{
    const RtlFieldInfo * field;
    PathNodeType nodeType;
    arrow::FieldPath nodePath;
    arrow::ArrayBuilder *structPtr;
    unsigned int childCount = 0;
    unsigned int childrenProcessed = 0;

    ArrayBuilderTracker(const RtlFieldInfo *_field, arrow::ArrayBuilder *_struct, PathNodeType _nodeType, arrow::FieldPath && _nodePath)
        : field(_field), nodeType(_nodeType), structPtr(_struct), nodePath(std::move(_nodePath))
    {
        if (nodeType == CPNTDataset)
            childCount = structPtr->num_children();
    }

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
    LargeListType,
    StructType,
    RealType,
    FixedSizeBinaryType
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
    arrow::Status Visit(const arrow::FixedSizeBinaryArray &array)
    {
        fixedSizeBinaryArr = &array;
        type = FixedSizeBinaryType;
        size = array.byte_width();
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
    arrow::Status Visit(const arrow::LargeListArray &array)
    {
        largeListArr = &array;
        type = LargeListType;
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
    const arrow::FixedSizeBinaryArray *fixedSizeBinaryArr = nullptr;
    const arrow::StringArray *stringArr = nullptr;
    const arrow::LargeStringArray *largeStringArr = nullptr;
    const arrow::BinaryArray *binArr = nullptr;
    const arrow::LargeBinaryArray *largeBinArr = nullptr;
    const arrow::Decimal128Array *decArr = nullptr;
    const arrow::Decimal256Array *largeDecArr = nullptr;
    const arrow::ListArray *listArr = nullptr;
    const arrow::LargeListArray *largeListArr = nullptr;
    const arrow::StructArray *structArr = nullptr;
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
    ParquetReader(const char *option, const char *_location, int _maxRowCountInTable, const char *_partitionFields, const IThorActivityContext *_activityCtx, const RtlTypeInfo *_expectedRecord);
    ~ParquetReader();

    arrow::Status processReadFile();
    bool shouldRead();
    __int64 next(TableColumns *&nextTable);

    bool getCursor(MemoryBuffer & cursor);
    void setCursor(MemoryBuffer & cursor);

private:
    arrow::Status openReadFile();
    __int64 readColumns(__int64 currTable);
    void splitTable(std::shared_ptr<arrow::Table> &table);
    std::shared_ptr<parquet::arrow::RowGroupReader> queryCurrentTable(__int64 currTable);
    arrow::Result<std::shared_ptr<arrow::Table>> queryRows();

private:
    // Count of processed rows and tables for both partitioned and regular files.
    __int64 tablesProcessed = 0;                                       // The number of tables processed when reading parquet files.
    __int64 totalRowsProcessed = 0;                                    // Total number of rows processed.
    __int64 rowsProcessed = 0;                                         // Current Row that has been read from the RowGroup.
    __int64 rowsCount = 0;                                             // The number of result rows in a given RowGroup read from the parquet file.

    // Partitioned file read location and size in rows.
    __int64 totalRowCount = 0;                                         // Total number of rows in a partition dataset to be read by the worker.
    __int64 startRow = 0;                                              // The starting row in a partitioned dataset.

    // Regular file read location and size in tables.
    __int64 tableCount = 0;                                            // The number of RowGroups to be read by the worker from the file that was opened for reading.
    __int64 startRowGroup = 0;                                         // The beginning RowGroup that is read by a worker.

    bool restoredCursor = false;                                       // True if reading from a restored file location. Skips check rowsProcessed == rowsCount to open the table at the current row location.
    size_t maxRowCountInTable = 0;                                     // Max table size set by user.
    std::string partOption;                                            // Begins with either read or write and ends with the partitioning type if there is one i.e. 'readhivepartition'.
    std::string location;                                              // Full path to location for reading parquet files. Can be a filename or directory.
    const RtlTypeInfo * expectedRecord = nullptr;                      // Expected record layout of Parquet file. Only available when used in the platform i.e. not available when used as a plugin.
    const IThorActivityContext *activityCtx = nullptr;                 // Context about the thor worker configuration.
    std::shared_ptr<arrow::dataset::Scanner> scanner = nullptr;        // Scanner for reading through partitioned files.
    std::shared_ptr<arrow::RecordBatchReader> rbatchReader = nullptr;                           // RecordBatchReader reads a dataset one record batch at a time. Must be kept alive for rbatchItr.
    arrow::RecordBatchReader::RecordBatchReaderIterator rbatchItr;                              // Iterator of RecordBatches when reading a partitioned dataset.
    std::vector<__int64> fileTableCounts;                                                       // Count of RowGroups in each open file to get the correct row group when reading specific parts of the file.
    std::vector<std::shared_ptr<parquet::arrow::FileReader>> parquetFileReaders;                // Vector of FileReaders that match the target file name. data0.parquet, data1.parquet, etc.
    std::shared_ptr<parquet::FileMetaData> currentTableMetadata = nullptr;                      // Parquet metadata for the current table.
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
    void updateRow();
    std::shared_ptr<arrow::NestedType> makeChildRecord(const RtlFieldInfo *field);
    arrow::Status fieldToNode(const RtlFieldInfo *field, std::vector<std::shared_ptr<arrow::Field>> &arrowFields);
    arrow::Status fieldsToSchema(const RtlTypeInfo *typeInfo);
    void beginSet(const RtlFieldInfo *field);
    void beginRow(const RtlFieldInfo *field);
    void endRow();
    arrow::Status checkDirContents();
    __int64 getMaxRowSize() {return maxRowCountInBatch;}
    arrow::ArrayBuilder *getFieldBuilder(const RtlFieldInfo *field);
    arrow::FieldPath getNestedFieldBuilder(const RtlFieldInfo *field, arrow::ArrayBuilder *&childBuilder);
    void addFieldToBuilder(const RtlFieldInfo *field, unsigned len, const char *data);

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
    std::vector<std::shared_ptr<ArrayBuilderTracker>> fieldBuilderStack;                        // Stack of field type specific column builders for nested structures.
    std::shared_ptr<arrow::RecordBatchBuilder> recordBatchBuilder = nullptr;                    // A convenient helper object containing type specific column builders for each field in the schema.
    arrow::dataset::FileSystemDatasetWriteOptions writeOptions;                                 // Write options for writing partitioned files.
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
    int64_t currArrayIndex();

private:
    __int64 currentRow;                                                             // The index in the arrow Array to read the current value.
    StringBuffer serialized;                                                        // Output string from serialization.
    TableColumns *resultRows = nullptr;                                             // A pointer to the result rows map where the left side are the field names for the columns and the right is an array of values.
    std::vector<ParquetColumnTracker> pathStack;                                    // ParquetColumnTracker keeps track of nested data when reading sets.
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
        parquetWriter->beginSet(field);
        return true;
    }
    virtual bool processBeginDataset(const RtlFieldInfo *field, unsigned rowsCount)
    {
        UNSUPPORTED("DATASET");
    }
    virtual bool processBeginRow(const RtlFieldInfo *field)
    {
        parquetWriter->beginRow(field);
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo *field)
    {
        parquetWriter->endRow();
    }
    virtual void processEndDataset(const RtlFieldInfo *field)
    {
        UNSUPPORTED("DATASET");
    }
    virtual void processEndRow(const RtlFieldInfo *field)
    {
        parquetWriter->endRow();
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
