#include "materialization.hpp"

#include "nbcd.hpp"
#include "jhtree.hpp"
#include "rtldynfield.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb::hpcc
{
namespace
{
static duckdb::Value materializeScalarValue(const RtlTypeInfo& type, const void* value, IBlobProvider* blobProvider);
static void writeRtlScalar(duckdb::Vector& vector, duckdb::idx_t row, const RtlTypeInfo& type, const void* value, IBlobProvider* blobProvider);

// Unsigned 64-bit HPCC integers exceed BIGINT, so they map to UBIGINT; all other integers map to BIGINT.
static bool isUnsigned64(const RtlTypeInfo& type)
{
    const RtlTypeInfo& valueType = *queryFilterValueType(&type);
    switch (valueType.getType())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
        return valueType.isUnsigned() && valueType.length == sizeof(uint64_t);
    default:
        return false;
    }
}

// Like DuckDB's own readers (e.g. Avro), values are stored straight into the output vector. A duckdb::Value, and the
// implicit cast SetValue applies to it, is only used when the value's type differs from the vector's type.
static void writeBoolean(duckdb::Vector& vector, duckdb::idx_t row, bool value)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::BOOLEAN)
        duckdb::FlatVector::GetData<bool>(vector)[row] = value;
    else
        vector.SetValue(row, duckdb::Value::BOOLEAN(value));
}

static void writeBigint(duckdb::Vector& vector, duckdb::idx_t row, int64_t value)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::BIGINT)
        duckdb::FlatVector::GetData<int64_t>(vector)[row] = value;
    else
        vector.SetValue(row, duckdb::Value::BIGINT(value));
}

static void writeUBigint(duckdb::Vector& vector, duckdb::idx_t row, uint64_t value)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::UBIGINT)
        duckdb::FlatVector::GetData<uint64_t>(vector)[row] = value;
    else
        vector.SetValue(row, duckdb::Value::UBIGINT(value));
}

static void writeDouble(duckdb::Vector& vector, duckdb::idx_t row, double value)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::DOUBLE)
        duckdb::FlatVector::GetData<double>(vector)[row] = value;
    else
        vector.SetValue(row, duckdb::Value::DOUBLE(value));
}

static void writeDecimal(duckdb::Vector& vector, duckdb::idx_t row, duckdb::hugeint_t unscaled, uint8_t width, uint8_t scale)
{
    const duckdb::LogicalType& type = vector.GetType();
    if (type.id() == duckdb::LogicalTypeId::DECIMAL && duckdb::DecimalType::GetWidth(type) == width && duckdb::DecimalType::GetScale(type) == scale)
    {
        switch (type.InternalType())
        {
        case duckdb::PhysicalType::INT16:
            duckdb::FlatVector::GetData<int16_t>(vector)[row] = duckdb::Hugeint::Cast<int16_t>(unscaled);
            return;
        case duckdb::PhysicalType::INT32:
            duckdb::FlatVector::GetData<int32_t>(vector)[row] = duckdb::Hugeint::Cast<int32_t>(unscaled);
            return;
        case duckdb::PhysicalType::INT64:
            duckdb::FlatVector::GetData<int64_t>(vector)[row] = duckdb::Hugeint::Cast<int64_t>(unscaled);
            return;
        case duckdb::PhysicalType::INT128:
            duckdb::FlatVector::GetData<duckdb::hugeint_t>(vector)[row] = unscaled;
            return;
        default:
            break;
        }
    }
    vector.SetValue(row, duckdb::Value::DECIMAL(unscaled, width, scale));
}

static void writeString(duckdb::Vector& vector, duckdb::idx_t row, const char* data, size32_t length)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::VARCHAR)
        duckdb::FlatVector::GetData<duckdb::string_t>(vector)[row] = duckdb::StringVector::AddString(vector, data, length);
    else
        vector.SetValue(row, duckdb::Value(std::string(data, length)));
}

static void writeUtf8String(duckdb::Vector& vector, duckdb::idx_t row, const char* data, size32_t length)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::VARCHAR)
        duckdb::FlatVector::GetData<duckdb::string_t>(vector)[row] = duckdb::StringVector::AddString(vector, data, length);
    else
        vector.SetValue(row, duckdb::Value(std::string(data, length)));
}

static void writeBlob(duckdb::Vector& vector, duckdb::idx_t row, const void* data, size32_t length)
{
    if (vector.GetType().id() == duckdb::LogicalTypeId::BLOB)
        duckdb::FlatVector::GetData<duckdb::string_t>(vector)[row] = duckdb::StringVector::AddStringOrBlob(vector, static_cast<const char*>(data), length);
    else
        vector.SetValue(row, duckdb::Value::BLOB(static_cast<const unsigned char*>(data), length));
}

static duckdb::hugeint_t readUnscaledDecimal(const void* value, size32_t size, unsigned width, unsigned scale, bool isUnsigned)
{
    if (!width || width > duckdb::Decimal::MAX_WIDTH_DECIMAL || scale > width)
        throw duckdb::NotImplementedException("hpcc_scan supports DECIMAL widths from 1 through %u", static_cast<unsigned>(duckdb::Decimal::MAX_WIDTH_DECIMAL));
    ::Decimal decimal;
    if (isUnsigned)
        decimal.setUDecimal(static_cast<byte>(size), static_cast<byte>(scale), value);
    else
        decimal.setDecimal(static_cast<byte>(size), static_cast<byte>(scale), value);

    if (width <= duckdb::Decimal::MAX_WIDTH_INT64)
        return isUnsigned ? duckdb::Hugeint::Convert(static_cast<uint64_t>(decimal.getUInt64())) : duckdb::Hugeint::Convert(static_cast<int64_t>(decimal.getInt64()));

    std::string decimalText(decimal.getStringLength() + 1, '\0');
    decimal.getCString(static_cast<size32_t>(decimalText.size()), decimalText.data());
    decimalText.resize(decimalText.size() - 1);
    const size_t decimalPoint = decimalText.find('.');
    unsigned fractionalDigits = 0;
    if (decimalPoint != std::string::npos)
    {
        fractionalDigits = static_cast<unsigned>(decimalText.size() - decimalPoint - 1);
        decimalText.erase(decimalPoint, 1);
    }
    if (fractionalDigits > scale)
        throw duckdb::InternalException("hpcc_scan decimal value exceeds its declared scale");
    decimalText.append(scale - fractionalDigits, '0');

    duckdb::hugeint_t unscaled;
    if (!duckdb::Hugeint::TryConvert(decimalText.c_str(), unscaled))
        throw duckdb::OutOfRangeException("hpcc_scan DECIMAL(%u,%u) value exceeds DuckDB's HUGEINT range", width, scale);
    return unscaled;
}

// Receives HPCC field callbacks for one row and writes them into the output vectors. Child datasets and sets append
// to the LIST's child vector and nested records fill the STRUCT's child vectors. A subtree whose shape does not match
// its vector's type is built as a duckdb::Value and cast on assignment, as the original Value-based conversion did.
class CScanVectorWriter : implements IFieldProcessor, public CInterface
{
    struct Target
    {
        duckdb::Vector* vector = nullptr; // null when the value belongs to the enclosing boxed frame
        duckdb::idx_t row = 0;
    };

    struct Frame
    {
        bool collection = false;
        bool boxed = false;
        Target target;
        duckdb::idx_t offset = 0;
        duckdb::idx_t count = 0;
        unsigned nextChild = 0;
        std::vector<duckdb::Value> values;
        std::vector<std::string> names;
    };

    duckdb::DataChunk& output;
    const std::vector<FieldDescriptor>& fields;
    IBlobProvider* blobProvider = nullptr;
    duckdb::idx_t rowIndex = 0;
    unsigned column = 0;
    unsigned currentColumn = 0;
    bool hasPending = false;
    size_t frameDepth = 0;
    Target pending;
    std::vector<Frame> frames;

    static bool isCompoundType(const RtlTypeInfo& type)
    {
        return type.getType() == type_record || type.getType() == type_row || type.getType() == type_set || type.getType() == type_table || type.getType() == type_groupedtable;
    }

    Target nextTarget(const RtlFieldInfo* field)
    {
        if (hasPending)
        {
            hasPending = false;
            return pending;
        }
        if (!frameDepth)
        {
            if (column >= output.ColumnCount())
                throw duckdb::InternalException("hpcc_scan produced more values than projected columns");
            currentColumn = column;
            return { &output.data[column++], rowIndex };
        }
        Frame& frame = frames[frameDepth - 1];
        if (frame.boxed)
        {
            if (!frame.collection)
                frame.names.emplace_back(field && field->name ? std::string(field->name) : "field" + std::to_string(frame.values.size()));
            return {};
        }
        duckdb::Vector& vector = *frame.target.vector;
        if (frame.collection)
        {
            const duckdb::idx_t index = frame.offset + frame.count++;
            duckdb::ListVector::Reserve(vector, index + 1);
            duckdb::ListVector::SetListSize(vector, index + 1);
            return { &duckdb::ListVector::GetEntry(vector), index };
        }
        auto& entries = duckdb::StructVector::GetEntries(vector);
        if (frame.nextChild >= entries.size())
            throw duckdb::InternalException("hpcc_scan nested record has more fields than its STRUCT column");
        return { entries[frame.nextChild++].get(), frame.target.row };
    }

    void setBoxed(const Target& target, duckdb::Value value)
    {
        if (target.vector)
            target.vector->SetValue(target.row, value);
        else
            frames[frameDepth - 1].values.push_back(std::move(value));
    }

    void beginNested(bool collection, const Target& target, duckdb::idx_t expectedElements)
    {
        if (frameDepth == frames.size())
            frames.emplace_back();
        Frame& frame = frames[frameDepth++];
        frame.collection = collection;
        frame.boxed = false;
        frame.target = target;
        frame.offset = 0;
        frame.count = 0;
        frame.nextChild = 0;
        frame.values.clear();
        frame.names.clear();
        const duckdb::LogicalTypeId expectedType = collection ? duckdb::LogicalTypeId::LIST : duckdb::LogicalTypeId::STRUCT;
        frame.boxed = !target.vector || target.vector->GetType().id() != expectedType;
        if (collection && !frame.boxed)
        {
            frame.offset = duckdb::ListVector::GetListSize(*target.vector);
            if (expectedElements)
                duckdb::ListVector::Reserve(*target.vector, frame.offset + expectedElements);
        }
    }

    void endNested(bool collection)
    {
        if (!frameDepth || frames[frameDepth - 1].collection != collection)
            throw duckdb::InternalException("hpcc_scan nested metadata callback mismatch");

        Frame& frame = frames[frameDepth - 1];
        --frameDepth;
        if (!frame.boxed)
        {
            duckdb::Vector& vector = *frame.target.vector;
            if (collection)
                duckdb::FlatVector::GetData<duckdb::list_entry_t>(vector)[frame.target.row] = duckdb::list_entry_t(frame.offset, frame.count);
            else
            {
                auto& entries = duckdb::StructVector::GetEntries(vector);
                for (unsigned index = frame.nextChild; index < entries.size(); ++index)
                    entries[index]->SetValue(frame.target.row, duckdb::Value());
            }
            return;
        }

        duckdb::Value value;
        if (collection)
        {
            value = frame.values.empty() ? duckdb::Value::LIST(duckdb::LogicalType::SQLNULL, {}) : duckdb::Value::LIST(std::move(frame.values));
        }
        else
        {
            duckdb::child_list_t<duckdb::Value> children;
            for (unsigned index = 0; index < frame.values.size(); ++index)
                children.emplace_back(std::move(frame.names[index]), std::move(frame.values[index]));
            value = duckdb::Value::STRUCT(std::move(children));
        }
        setBoxed(frame.target, std::move(value));
    }

    void writeNestedBlob(const RtlFieldInfo* field, const byte* blob)
    {
        const Target target = nextTarget(field);
        if (!blob)
        {
            setBoxed(target, duckdb::Value());
            return;
        }
        const RtlTypeInfo& childType = *field->type->queryChildType();
        if (isCompoundType(childType))
        {
            const bool targetIsList = target.vector && target.vector->GetType().id() == duckdb::LogicalTypeId::LIST;
            if (targetIsList && childType.getType() != type_set && childType.getType() != type_table && childType.getType() != type_groupedtable)
            {
                beginNested(true, target, 1);
                childType.process(blob, blob, field, *this);
                endNested(true);
            }
            else
            {
                pending = target;
                hasPending = true;
                childType.process(blob, blob, field, *this);
            }
        }
        else if (target.vector)
            writeRtlScalar(*target.vector, target.row, childType, blob, blobProvider);
        else
            setBoxed(target, materializeScalarValue(childType, blob, blobProvider));
    }

    const byte* lookupNestedBlob(unsigned __int64 id)
    {
        try
        {
            return blobProvider->lookupBlob(id);
        }
        catch (IException* exception)
        {
            StringBuffer message;
            exception->errorMessage(message);
            exception->Release();
            throw duckdb::IOException("hpcc_scan could not load nested KEY blob: %s", message.str());
        }
    }

    [[noreturn]] void unsupported(const char* kind)
    {
        throw duckdb::NotImplementedException("hpcc_scan does not yet convert %s fields", kind);
    }

    unsigned decimalWidth() const
    {
        const duckdb::LogicalType& type = fields.at(currentColumn).duckdbType;
        return type.id() == duckdb::LogicalTypeId::DECIMAL ? duckdb::DecimalType::GetWidth(type) : 0;
    }

    void writeStringValue(const RtlFieldInfo* field, const char* value, size32_t len)
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeString(*target.vector, target.row, value, len);
        else
            setBoxed(target, duckdb::Value(std::string(value, len)));
    }

    void writeUtf8Value(const RtlFieldInfo* field, const char* value, size32_t len)
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeUtf8String(*target.vector, target.row, value, len);
        else
            setBoxed(target, duckdb::Value(std::string(value, len)));
    }

    void writeBigintValue(const RtlFieldInfo* field, int64_t value)
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeBigint(*target.vector, target.row, value);
        else
            setBoxed(target, duckdb::Value::BIGINT(value));
    }

    void writeUBigintValue(const RtlFieldInfo* field, uint64_t value)
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeUBigint(*target.vector, target.row, value);
        else
            setBoxed(target, duckdb::Value::UBIGINT(value));
    }

    void writeDecimalValue(const RtlFieldInfo* field, duckdb::hugeint_t unscaled, unsigned width, unsigned precision)
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeDecimal(*target.vector, target.row, unscaled, static_cast<uint8_t>(width), static_cast<uint8_t>(precision));
        else
            setBoxed(target, duckdb::Value::DECIMAL(unscaled, static_cast<uint8_t>(width), static_cast<uint8_t>(precision)));
    }

public:
    IMPLEMENT_IINTERFACE;

    CScanVectorWriter(duckdb::DataChunk& _output, const std::vector<FieldDescriptor>& _fields, IBlobProvider* _blobProvider) : output(_output), fields(_fields), blobProvider(_blobProvider)
    {
    }

    void startRow(duckdb::idx_t row)
    {
        rowIndex = row;
        column = 0;
        hasPending = false;
        frameDepth = 0;
    }

    void processString(unsigned len, const char* value, const RtlFieldInfo* field) override
    {
        writeStringValue(field, value, len);
    }
    void processBool(bool value, const RtlFieldInfo* field) override
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeBoolean(*target.vector, target.row, value);
        else
            setBoxed(target, duckdb::Value::BOOLEAN(value));
    }
    void processData(unsigned len, const void* value, const RtlFieldInfo* field) override
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeBlob(*target.vector, target.row, value, len);
        else
            setBoxed(target, duckdb::Value::BLOB(static_cast<const unsigned char*>(value), len));
    }
    void processInt(__int64 value, const RtlFieldInfo* field) override
    {
        if (blobProvider && field && field->type && field->type->getType() == type_blob)
        {
            const byte* blob = lookupNestedBlob(static_cast<unsigned __int64>(value));
            if (!blob || field->type->queryChildType())
                writeNestedBlob(field, blob);
            return;
        }
        writeBigintValue(field, value);
    }
    void processUInt(unsigned __int64 value, const RtlFieldInfo* field) override
    {
        if (field && field->type && isUnsigned64(*field->type))
            writeUBigintValue(field, value);
        else
            writeBigintValue(field, static_cast<__int64>(value));
    }
    void processReal(double value, const RtlFieldInfo* field) override
    {
        const Target target = nextTarget(field);
        if (target.vector)
            writeDouble(*target.vector, target.row, value);
        else
            setBoxed(target, duckdb::Value::DOUBLE(value));
    }
    void processDecimal(const void* value, unsigned length, unsigned precision, const RtlFieldInfo* field) override
    {
        unsigned width = field && field->type ? field->type->getDecimalDigits() : decimalWidth();
        if (!width || width > duckdb::Decimal::MAX_WIDTH_DECIMAL || precision > width)
            unsupported("decimal");
        writeDecimalValue(field, readUnscaledDecimal(value, length, width, precision, false), width, precision);
    }
    void processUDecimal(const void* value, unsigned length, unsigned precision, const RtlFieldInfo* field) override
    {
        unsigned width = field && field->type ? field->type->getDecimalDigits() : decimalWidth();
        if (!width || width > duckdb::Decimal::MAX_WIDTH_DECIMAL || precision > width)
            unsupported("unsigned decimal");
        writeDecimalValue(field, readUnscaledDecimal(value, length, width, precision, true), width, precision);
    }
    void processUnicode(unsigned len, const UChar* value, const RtlFieldInfo* field) override
    {
        unsigned utf8Length = 0;
        rtlDataAttr utf8Value;
        rtlUnicodeToUtf8X(utf8Length, utf8Value.refstr(), len, value);
        writeUtf8Value(field, utf8Value.getstr(), rtlUtf8Size(utf8Length, utf8Value.getstr()));
    }
    void processQString(unsigned len, const char* value, const RtlFieldInfo* field) override
    {
        writeStringValue(field, value, len);
    }
    void processUtf8(unsigned len, const char* value, const RtlFieldInfo* field) override
    {
        writeUtf8Value(field, value, len);
    }

    void processNull()
    {
        setBoxed(nextTarget(nullptr), duckdb::Value());
    }

    void processBlobField(const RtlFieldInfo* field, const void* value)
    {
        if (!blobProvider || !field || !field->type || !field->type->queryChildType())
            throw duckdb::NotImplementedException("hpcc_scan cannot resolve a nested KEY blob");
        writeNestedBlob(field, lookupNestedBlob(static_cast<unsigned __int64>(field->type->getInt(value))));
    }

    bool processBeginSet(const RtlFieldInfo* field, unsigned numElements, bool isAll, const byte*) override
    {
        if (isAll)
            unsupported("unbounded set");
        beginNested(true, nextTarget(field), numElements);
        return true;
    }
    bool processBeginDataset(const RtlFieldInfo* field, unsigned numRows) override
    {
        beginNested(true, nextTarget(field), numRows);
        return true;
    }
    bool processBeginRow(const RtlFieldInfo* field) override
    {
        beginNested(false, nextTarget(field), 0);
        return true;
    }
    void processEndSet(const RtlFieldInfo*) override { endNested(true); }
    void processEndDataset(const RtlFieldInfo*) override { endNested(true); }
    void processEndRow(const RtlFieldInfo*) override { endNested(false); }
};

static FieldDescriptor mapRtlFieldInternal(const RtlFieldInfo& field, const char* prefix)
{
    FieldDescriptor descriptor;
    descriptor.fieldPath = prefix ? std::string(prefix) + "." + (field.name ? field.name : "field") : (field.name ? field.name : "field");
    const RtlTypeInfo& type = *field.type;
    const RtlTypeInfo* mappedType = &type;
    if (type.getType() == type_blob && type.queryChildType())
        mappedType = type.queryChildType();
    switch (mappedType->getType())
    {
    case type_record:
    case type_row:
    case type_set:
    case type_table:
    case type_groupedtable:
        break;
    default:
        mapScalarFieldType(descriptor, static_cast<type_t>(mappedType->getType()), isUnsigned64(type),
            mappedType->getType() == type_decimal ? mappedType->getDecimalDigits() : 0,
            mappedType->getType() == type_decimal ? mappedType->getDecimalPrecision() : 0);
        break;
    }
    const RtlTypeInfo* childType = type.getType() == type_blob ? mappedType : type.queryChildType();
    if (childType && (childType->getType() == type_set || childType->getType() == type_table || childType->getType() == type_groupedtable))
        childType = childType->queryChildType();
    const RtlFieldInfo* const* childFields = childType ? childType->queryFields() : nullptr;
    if (childFields)
    {
        for (unsigned index = 0; childFields[index]; ++index)
            descriptor.children.push_back(mapRtlFieldInternal(*childFields[index], descriptor.fieldPath.c_str()));
    }
    switch (mappedType->getType())
    {
    case type_record:
    case type_row:
    case type_set:
    case type_table:
    case type_groupedtable:
        descriptor.duckdbType = mapNestedType(static_cast<type_t>(mappedType->getType()), descriptor.children);
        break;
    }
    return descriptor;
}

static std::vector<FieldDescriptor> appendOfflineScanColumnsInternal(IOutputMetaData& layoutMetadata, const std::vector<FieldDescriptor>& descriptorFields, duckdb::vector<duckdb::LogicalType>& returnTypes, duckdb::vector<std::string>& names)
{
    std::vector<FieldDescriptor> descriptors = descriptorFields;
    if (descriptors.empty())
    {
        const RtlRecord& record = layoutMetadata.queryRecordAccessor(true);
        for (unsigned index = 0; index < record.getNumFields(); ++index)
            descriptors.push_back(mapRtlFieldInternal(*record.queryOriginalField(index), nullptr));
    }
    if (descriptors.empty())
        throw duckdb::BinderException("hpcc_scan offline overload received an empty record layout");
    for (const auto& descriptor : descriptors)
    {
        returnTypes.push_back(descriptor.duckdbType);
        names.emplace_back(descriptor.fieldPath);
    }
    return descriptors;
}

static duckdb::Value materializeScalarValue(const RtlTypeInfo& type, const void* value, IBlobProvider* blobProvider)
{
    switch (type.getType())
    {
    case type_boolean:
        return duckdb::Value::BOOLEAN(type.getInt(value) != 0);
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
    case type_keyedint:
        if (isUnsigned64(type))
            return duckdb::Value::UBIGINT(static_cast<uint64_t>(type.getInt(value)));
        return duckdb::Value::BIGINT(type.getInt(value));
    case type_real:
        return duckdb::Value::DOUBLE(type.getReal(value));
    case type_decimal:
        return duckdb::Value::DECIMAL(readUnscaledDecimal(value, type.size(static_cast<const byte*>(value), static_cast<const byte*>(value)),
            type.getDecimalDigits(), type.getDecimalPrecision(), type.isUnsigned()),
            static_cast<uint8_t>(type.getDecimalDigits()), static_cast<uint8_t>(type.getDecimalPrecision()));
    case type_string:
    case type_varstring:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
    {
        size32_t length = 0;
        rtlDataAttr text;
        type.getString(length, text.refstr(), value);
        return duckdb::Value(std::string(text.getstr(), length));
    }
    case type_data:
    {
        size32_t length = 0;
        rtlDataAttr data;
        type.getString(length, data.refstr(), value);
        return duckdb::Value::BLOB(static_cast<const unsigned char*>(data.getbytes()), length);
    }
    case type_blob:
    {
        if (!blobProvider)
            throw duckdb::NotImplementedException("hpcc_scan cannot resolve a blob without a KEY blob provider");
        const byte* blob = nullptr;
        try
        {
            blob = blobProvider->lookupBlob(static_cast<unsigned __int64>(type.getInt(value)));
        }
        catch (IException* exception)
        {
            StringBuffer message;
            exception->errorMessage(message);
            exception->Release();
            throw duckdb::IOException("hpcc_scan could not load KEY blob: %s", message.str());
        }
        if (!blob)
            return duckdb::Value();
        return materializeScalarValue(*type.queryChildType(), blob, blobProvider);
    }
    default:
        throw duckdb::NotImplementedException("hpcc_scan direct conversion does not support nested or decimal fields");
    }
}

static void writeRtlScalar(duckdb::Vector& vector, duckdb::idx_t row, const RtlTypeInfo& type, const void* value, IBlobProvider* blobProvider)
{
    switch (type.getType())
    {
    case type_boolean:
        writeBoolean(vector, row, type.getInt(value) != 0);
        return;
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
    case type_keyedint:
        if (isUnsigned64(type))
            writeUBigint(vector, row, static_cast<uint64_t>(type.getInt(value)));
        else
            writeBigint(vector, row, type.getInt(value));
        return;
    case type_real:
        writeDouble(vector, row, type.getReal(value));
        return;
    case type_decimal:
        writeDecimal(vector, row, readUnscaledDecimal(value, type.size(static_cast<const byte*>(value), static_cast<const byte*>(value)),
            type.getDecimalDigits(), type.getDecimalPrecision(), type.isUnsigned()),
            static_cast<uint8_t>(type.getDecimalDigits()), static_cast<uint8_t>(type.getDecimalPrecision()));
        return;
    case type_string:
    case type_varstring:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    {
        size32_t length = 0;
        rtlDataAttr text;
        type.getString(length, text.refstr(), value);
        writeString(vector, row, text.getstr(), length);
        return;
    }
    case type_utf8:
    {
        size32_t length = 0;
        rtlDataAttr text;
        type.getString(length, text.refstr(), value);
        writeUtf8String(vector, row, text.getstr(), length);
        return;
    }
    case type_data:
    {
        size32_t length = 0;
        rtlDataAttr data;
        type.getString(length, data.refstr(), value);
        writeBlob(vector, row, data.getdata(), length);
        return;
    }
    default:
        vector.SetValue(row, materializeScalarValue(type, value, blobProvider));
        return;
    }
}

// conditions caches IFBLOCK conditions for the current row; it is null when the record has no IFBLOCKs.
static bool isExcludedIfBlockField(const RtlRecord& record, const byte* row, unsigned fieldIndex, byte* conditions)
{
    if (!conditions)
        return false;
    const RtlFieldInfo* field = record.queryField(fieldIndex);
    if (!(field->flags & RFTMinifblock))
        return false;
    return record.excluded(field, row, conditions);
}

static void materializeScalarRow(const RtlRecord& record, const RtlRow& row, const std::vector<unsigned>& projectedColumns, duckdb::DataChunk& output, duckdb::idx_t rowIndex, byte* conditions, IBlobProvider* blobProvider)
{
    for (unsigned outputIndex = 0; outputIndex < projectedColumns.size(); ++outputIndex)
    {
        const unsigned fieldIndex = projectedColumns[outputIndex];
        if (isExcludedIfBlockField(record, row.queryRow(), fieldIndex, conditions))
        {
            output.data[outputIndex].SetValue(rowIndex, duckdb::Value());
            continue;
        }
        writeRtlScalar(output.data[outputIndex], rowIndex, *record.queryType(fieldIndex), row.queryField(fieldIndex), blobProvider);
    }
}


} // namespace

FieldDescriptor mapRtlField(const RtlFieldInfo& field, const char* prefix)
{
    return mapRtlFieldInternal(field, prefix);
}

std::vector<FieldDescriptor> mapRtlRecord(const RtlRecord& record)
{
    std::vector<FieldDescriptor> descriptors;
    descriptors.reserve(record.getNumFields());
    for (unsigned index = 0; index < record.getNumFields(); ++index)
        descriptors.push_back(mapRtlFieldInternal(*record.queryOriginalField(index), nullptr));
    return descriptors;
}

std::vector<FieldDescriptor> appendOfflineScanColumns(IOutputMetaData& layoutMetadata, const std::vector<FieldDescriptor>& descriptorFields, duckdb::vector<duckdb::LogicalType>& returnTypes, duckdb::vector<std::string>& names)
{
    return appendOfflineScanColumnsInternal(layoutMetadata, descriptorFields, returnTypes, names);
}

class CScanMaterializer::Impl
{
public:
    Impl(duckdb::DataChunk& output, const std::vector<FieldDescriptor>& projectedDescriptors, IBlobProvider* blobProvider, bool hasNestedProjection)
        : output(output), blobProvider(blobProvider), hasNestedProjection(hasNestedProjection)
    {
        if (hasNestedProjection)
            writer = std::make_unique<CScanVectorWriter>(output, projectedDescriptors, blobProvider);
    }

    void materialize(const RtlRecord& record, const RtlRow& row, const byte* rawRow, const std::vector<unsigned>& projectedColumns, duckdb::idx_t rowIndex, byte* conditions)
    {
        if (!hasNestedProjection)
        {
            materializeScalarRow(record, row, projectedColumns, output, rowIndex, conditions, blobProvider);
            return;
        }

        writer->startRow(rowIndex);
        for (unsigned index = 0; index < projectedColumns.size(); ++index)
        {
            const unsigned fieldIndex = projectedColumns[index];
            const RtlFieldInfo* field = record.queryOriginalField(fieldIndex);
            if (!field)
                throw duckdb::InternalException("hpcc_scan could not resolve a projected field");
            if (isExcludedIfBlockField(record, rawRow, fieldIndex, conditions))
            {
                writer->processNull();
                continue;
            }
            const byte* fieldValue = row.queryField(fieldIndex);
            if (field->type && field->type->getType() == type_blob)
                writer->processBlobField(field, fieldValue);
            else
                field->process(fieldValue, rawRow, *writer);
        }
    }

private:
    duckdb::DataChunk& output;
    IBlobProvider* blobProvider;
    bool hasNestedProjection;
    std::unique_ptr<CScanVectorWriter> writer;
};

CScanMaterializer::CScanMaterializer(duckdb::DataChunk& output, const std::vector<FieldDescriptor>& projectedDescriptors, IBlobProvider* blobProvider, bool hasNestedProjection)
    : impl(std::make_unique<Impl>(output, projectedDescriptors, blobProvider, hasNestedProjection))
{
}

CScanMaterializer::~CScanMaterializer() = default;

void CScanMaterializer::materialize(const RtlRecord& record, const RtlRow& row, const byte* rawRow, const std::vector<unsigned>& projectedColumns, duckdb::idx_t rowIndex, byte* conditions)
{
    impl->materialize(record, row, rawRow, projectedColumns, rowIndex, conditions);
}

} // namespace duckdb::hpcc
