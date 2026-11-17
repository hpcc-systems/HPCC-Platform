#include "typemapping.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "materialization.hpp"

#include <string>
#include <vector>
#endif

#include <unordered_map>

namespace duckdb::hpcc
{

static std::string appendFieldPath(const char* prefix, const char* name)
{
    std::string path = prefix ? prefix : "";
    if (!path.empty())
        path.append(".");
    path.append(name ? name : "field");
    return path;
}

static std::string makeUniqueFieldPath(const std::string& proposedPath, std::unordered_map<std::string, unsigned>& fieldPaths)
{
    auto [entry, inserted] = fieldPaths.emplace(proposedPath, 2);
    if (inserted)
        return proposedPath;

    unsigned& nextSuffix = entry->second;
    for (;;)
    {
        std::string candidate = proposedPath;
        candidate.append("_").append(std::to_string(nextSuffix++));
        if (fieldPaths.emplace(candidate, 2).second)
            return candidate;
    }
}

duckdb::LogicalType mapDecimalType(unsigned width, unsigned scale)
{
    if (width && width <= duckdb::Decimal::MAX_WIDTH_DECIMAL && scale <= width)
        return duckdb::LogicalType::DECIMAL(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
    throw duckdb::BinderException("hpcc_scan cannot bind DECIMAL(%u,%u): supported widths are 1 through %u and scale must not exceed width",
        width, scale, static_cast<unsigned>(duckdb::Decimal::MAX_WIDTH_DECIMAL));
}

duckdb::LogicalType mapNestedType(type_t sourceType, const std::vector<FieldDescriptor>& children)
{
    const bool isRow = sourceType == type_record || sourceType == type_row;
    if (!isRow && sourceType != type_dictionary && children.size() == 1)
        return duckdb::LogicalType::LIST(children.front().duckdbType);
    duckdb::child_list_t<duckdb::LogicalType> fields;
    for (const auto& child : children)
    {
        const size_t separator = child.fieldPath.rfind('.');
        const std::string name = separator == std::string::npos ? child.fieldPath : child.fieldPath.substr(separator + 1);
        fields.emplace_back(name, child.duckdbType);
    }
    if (isRow)
        return duckdb::LogicalType::STRUCT(std::move(fields));
    return duckdb::LogicalType::LIST(fields.empty() ? duckdb::LogicalType::VARCHAR : duckdb::LogicalType::STRUCT(std::move(fields)));
}

void mapScalarFieldType(FieldDescriptor& descriptor, type_t sourceType, bool unsigned64, unsigned decimalDigits, unsigned decimalPrecision)
{
    descriptor.fallback = false;
    switch (sourceType)
    {
    case type_boolean:
        descriptor.duckdbType = duckdb::LogicalType::BOOLEAN;
        break;
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
    case type_keyedint:
        descriptor.duckdbType = unsigned64 ? duckdb::LogicalType::UBIGINT : duckdb::LogicalType::BIGINT;
        break;
    case type_real:
        descriptor.duckdbType = duckdb::LogicalType::DOUBLE;
        break;
    case type_string:
    case type_varstring:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        descriptor.duckdbType = duckdb::LogicalType::VARCHAR;
        break;
    case type_data:
    case type_blob:
        descriptor.duckdbType = duckdb::LogicalType::BLOB;
        break;
    case type_date:
        descriptor.duckdbType = duckdb::LogicalType::DATE;
        break;
    case type_decimal:
        descriptor.duckdbType = mapDecimalType(decimalDigits, decimalPrecision);
        break;
    default:
        descriptor.duckdbType = duckdb::LogicalType::VARCHAR;
        descriptor.fallback = true;
        break;
    }
}

FieldDescriptor TypeMapper::mapField(const char* fieldPath, ITypeInfo* sourceType, std::vector<Warning>& warnings) const
{
    FieldDescriptor descriptor;
    descriptor.fieldPath = fieldPath ? fieldPath : "";
    const type_t type = sourceType ? sourceType->getTypeCode() : type_none;
    switch (type)
    {
    case type_record:
    case type_row:
    case type_set:
    case type_table:
    case type_groupedtable:
    case type_dictionary:
        descriptor.duckdbType = mapNestedType(type, descriptor.children);
        break;
    default:
    {
        const bool isInteger = type == type_int || type == type_swapint || type == type_packedint || type == type_filepos || type == type_keyedint;
        const bool unsigned64 = isInteger && !sourceType->isSigned() && sourceType->queryPromotedType()->getSize() == sizeof(uint64_t);
        mapScalarFieldType(descriptor, type, unsigned64, type == type_decimal ? sourceType->getDigits() : 0, type == type_decimal ? sourceType->getPrecision() : 0);
        if (descriptor.fallback)
            warnings.push_back({ "UNSUPPORTED_FIELD_TYPE", descriptor.fieldPath, "Unsupported HPCC field type mapped to VARCHAR" });
        break;
    }
    }
    return descriptor;
}

static void mapRecordFields(const TypeMapper& mapper, const IDefRecordElement* record, const char* prefix, std::vector<FieldDescriptor>& fields, std::vector<Warning>& warnings, std::unordered_map<std::string, unsigned>& fieldPaths)
{
    if (!record)
        return;

    for (unsigned index = 0; index < record->numChildren(); ++index)
    {
        const IDefRecordElement* child = record->queryChild(index);
        if (!child)
            continue;

        const char* fieldName = child->queryName() ? str(child->queryName()) : nullptr;
        const std::string fieldPath = appendFieldPath(prefix, fieldName);
        ITypeInfo* childType = child->queryType();
        if (childType && (childType->getTypeCode() == type_record || childType->getTypeCode() == type_set || childType->getTypeCode() == type_table || childType->getTypeCode() == type_groupedtable || childType->getTypeCode() == type_row || childType->getTypeCode() == type_dictionary))
        {
            const std::string uniqueFieldPath = makeUniqueFieldPath(fieldPath, fieldPaths);
            FieldDescriptor descriptor = mapper.mapField(uniqueFieldPath.c_str(), childType, warnings);
            mapper.mapRecord(child, uniqueFieldPath.c_str(), descriptor.children, warnings);
            descriptor.duckdbType = mapNestedType(childType->getTypeCode(), descriptor.children);
            fields.push_back(std::move(descriptor));
            continue;
        }
        if (child->numChildren() > 0)
        {
            mapRecordFields(mapper, child, fieldPath.c_str(), fields, warnings, fieldPaths);
            continue;
        }

        const std::string uniqueFieldPath = makeUniqueFieldPath(fieldPath, fieldPaths);
        fields.push_back(mapper.mapField(uniqueFieldPath.c_str(), child->queryType(), warnings));
    }
}

void TypeMapper::mapRecord(const IDefRecordElement* record, const char* prefix, std::vector<FieldDescriptor>& fields, std::vector<Warning>& warnings) const
{
    if (!record)
        return;

    std::unordered_map<std::string, unsigned> fieldPaths;
    fieldPaths.reserve(fields.size() + record->numChildren());
    for (const auto& field : fields)
        fieldPaths.emplace(field.fieldPath, 2);
    mapRecordFields(*this, record, prefix, fields, warnings, fieldPaths);
}

#ifdef _USE_CPPUNIT
class WsSql2TypeMappingTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WsSql2TypeMappingTests);
    CPPUNIT_TEST(testFieldTypeMapping);
    CPPUNIT_TEST(testCommonFieldTypes);
    CPPUNIT_TEST(testDecimalTypes);
    CPPUNIT_TEST(testNestedTypes);
    CPPUNIT_TEST(testUniqueFieldPaths);
    CPPUNIT_TEST(testWideRecord);
    CPPUNIT_TEST(testRuntimeTypeMapping);
    CPPUNIT_TEST_SUITE_END();

public:
    void testRuntimeTypeMapping()
    {
        TypeMapper mapper;
        std::vector<Warning> warnings;
        auto checkMapping = [&](ITypeInfo* sourceType, const RtlTypeInfo& runtimeType)
            {
                const RtlFieldInfo field("value", nullptr, &runtimeType);
                const FieldDescriptor expected = mapper.mapField("value", sourceType, warnings);
                const FieldDescriptor actual = mapRtlField(field, nullptr);
                CPPUNIT_ASSERT_EQUAL(expected.fieldPath, actual.fieldPath);
                CPPUNIT_ASSERT(expected.duckdbType == actual.duckdbType);
                CPPUNIT_ASSERT_EQUAL(expected.fallback, actual.fallback);
            };
        for (unsigned size = 1; size <= 8; ++size)
        {
            for (bool isSigned : { false, true })
            {
                Owned<ITypeInfo> sourceType = makeIntType(size, isSigned);
                const RtlIntTypeInfo runtimeType(type_int | (isSigned ? 0 : type_unsigned), size);
                checkMapping(sourceType, runtimeType);
            }
        }
        for (unsigned digits : { 8U, 18U })
        {
            Owned<ITypeInfo> sourceType = makeDecimalType(digits, 2, true);
            const RtlDecimalTypeInfo runtimeType(type_decimal, digits | (2U << 16));
            checkMapping(sourceType, runtimeType);
        }
        Owned<ITypeInfo> boolType = makeBoolType();
        const RtlBoolTypeInfo runtimeBool(type_boolean, 1);
        checkMapping(boolType, runtimeBool);
        Owned<ITypeInfo> realType = makeRealType(8);
        const RtlRealTypeInfo runtimeReal(type_real, 8);
        checkMapping(realType, runtimeReal);
        Owned<ITypeInfo> stringType = makeStringType(10);
        const RtlStringTypeInfo runtimeString(type_string, 10);
        checkMapping(stringType, runtimeString);
        Owned<ITypeInfo> dataType = makeDataType(10);
        const RtlDataTypeInfo runtimeData(type_data, 10);
        checkMapping(dataType, runtimeData);
        CPPUNIT_ASSERT(warnings.empty());
        Owned<ITypeInfo> unsupportedType = makeVoidType();
        const RtlUnimplementedTypeInfo runtimeUnsupported(type_void, 0);
        checkMapping(unsupportedType, runtimeUnsupported);
        CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, warnings.size());
    }

    void testUniqueFieldPaths()
    {
        std::unordered_map<std::string, unsigned> fieldPaths;
        CPPUNIT_ASSERT_EQUAL(std::string("id"), makeUniqueFieldPath("id", fieldPaths));
        CPPUNIT_ASSERT_EQUAL(std::string("id_2"), makeUniqueFieldPath("id_2", fieldPaths));
        CPPUNIT_ASSERT_EQUAL(std::string("id_3"), makeUniqueFieldPath("id", fieldPaths));
        CPPUNIT_ASSERT_EQUAL(std::string("id_2_2"), makeUniqueFieldPath("id_2", fieldPaths));
        for (unsigned suffix = 4; suffix <= 4000; ++suffix)
            CPPUNIT_ASSERT_EQUAL(std::string("id_") + std::to_string(suffix), makeUniqueFieldPath("id", fieldPaths));
    }

    void testWideRecord()
    {
        TypeMapper mapper;
        std::vector<Warning> warnings;
        Owned<ITypeInfo> integerType = makeIntType(8, true);
        Owned<IDefRecordBuilder> builder = createDErecord(0);
        for (unsigned index = 0; index < 4000; ++index)
        {
            const std::string name = "column" + std::to_string(index);
            builder->addChildOwn(createDEfield(createAtom(name.c_str()), integerType, nullptr));
        }
        Owned<IDefRecordElement> record = builder->close();
        std::vector<FieldDescriptor> fields = { mapper.mapField("column0", integerType, warnings),
            mapper.mapField("column0_2", integerType, warnings) };
        mapper.mapRecord(record, nullptr, fields, warnings);
        CPPUNIT_ASSERT_EQUAL(size_t{ 4002 }, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("column0_3"), fields[2].fieldPath);
        for (unsigned index = 1; index < 4000; ++index)
        {
            CPPUNIT_ASSERT_EQUAL(std::string("column") + std::to_string(index), fields[index + 2].fieldPath);
            CPPUNIT_ASSERT(fields[index + 2].duckdbType == duckdb::LogicalType::BIGINT);
        }
        CPPUNIT_ASSERT(warnings.empty());
    }

    void testFieldTypeMapping()
    {
        TypeMapper mapper;
        std::vector<Warning> warnings;

        Owned<ITypeInfo> integerType = makeIntType(8, true);
        const FieldDescriptor integerField = mapper.mapField("id", integerType, warnings);
        CPPUNIT_ASSERT(integerField.duckdbType == duckdb::LogicalType::BIGINT);
        CPPUNIT_ASSERT(!integerField.fallback);
        CPPUNIT_ASSERT(warnings.empty());
        for (unsigned size = 1; size <= 8; ++size)
        {
            Owned<ITypeInfo> signedType = makeIntType(size, true);
            Owned<ITypeInfo> unsignedType = makeIntType(size, false);
            Owned<ITypeInfo> swappedType = makeSwapIntType(size, false);
            Owned<ITypeInfo> packedType = makePackedIntType(size, false);
            const duckdb::LogicalType expectedUnsignedType = size == 8 ? duckdb::LogicalType::UBIGINT : duckdb::LogicalType::BIGINT;
            CPPUNIT_ASSERT(mapper.mapField("signed", signedType, warnings).duckdbType == duckdb::LogicalType::BIGINT);
            CPPUNIT_ASSERT(mapper.mapField("unsigned", unsignedType, warnings).duckdbType == expectedUnsignedType);
            CPPUNIT_ASSERT(mapper.mapField("swapped", swappedType, warnings).duckdbType == expectedUnsignedType);
            CPPUNIT_ASSERT(mapper.mapField("packed", packedType, warnings).duckdbType == expectedUnsignedType);
        }
        CPPUNIT_ASSERT(warnings.empty());

        Owned<ITypeInfo> unsupportedType = makeVoidType();
        const FieldDescriptor unsupportedField = mapper.mapField("payload", unsupportedType, warnings);
        CPPUNIT_ASSERT(unsupportedField.duckdbType == duckdb::LogicalType::VARCHAR);
        CPPUNIT_ASSERT(unsupportedField.fallback);
        CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, warnings.size());
        CPPUNIT_ASSERT_EQUAL(std::string("UNSUPPORTED_FIELD_TYPE"), warnings.front().code);
        CPPUNIT_ASSERT_EQUAL(std::string("payload"), warnings.front().fieldPath);
        CPPUNIT_ASSERT(mapper.mapField("missing", nullptr, warnings).fallback);
        CPPUNIT_ASSERT_EQUAL(size_t{ 2 }, warnings.size());
    }

    void testCommonFieldTypes()
    {
        TypeMapper mapper;
        std::vector<Warning> warnings;

        Owned<ITypeInfo> booleanType = makeBoolType();
        Owned<ITypeInfo> realType = makeRealType(4);
        Owned<ITypeInfo> doubleType = makeRealType(8);
        Owned<ITypeInfo> stringType = makeStringType(10);
        Owned<ITypeInfo> unicodeType = makeUnicodeType(10, nullptr);
        Owned<ITypeInfo> dataType = makeDataType(64);
        CPPUNIT_ASSERT(mapper.mapField("flag", booleanType, warnings).duckdbType == duckdb::LogicalType::BOOLEAN);
        CPPUNIT_ASSERT(mapper.mapField("single", realType, warnings).duckdbType == duckdb::LogicalType::DOUBLE);
        CPPUNIT_ASSERT(mapper.mapField("double", doubleType, warnings).duckdbType == duckdb::LogicalType::DOUBLE);
        CPPUNIT_ASSERT(mapper.mapField("firstName", stringType, warnings).duckdbType == duckdb::LogicalType::VARCHAR);
        CPPUNIT_ASSERT(mapper.mapField("unicode", unicodeType, warnings).duckdbType == duckdb::LogicalType::VARCHAR);
        CPPUNIT_ASSERT(mapper.mapField("payload", dataType, warnings).duckdbType == duckdb::LogicalType::BLOB);
        CPPUNIT_ASSERT(warnings.empty());
    }

    void testDecimalTypes()
    {
        TypeMapper mapper;
        std::vector<Warning> warnings;
        const unsigned decimalWidthsAndScales[][2] = {
            { 1, 0 }, { 1, 1 }, { 8, 0 }, { 8, 8 }, { 18, 0 }, { 18, 18 },
            { 19, 2 }, { 20, 2 }, { 32, 2 }, { 38, 6 }
        };
        for (bool isSigned : { false, true })
        {
            for (const auto& widthAndScale : decimalWidthsAndScales)
            {
                const unsigned width = widthAndScale[0];
                const unsigned scale = widthAndScale[1];
                Owned<ITypeInfo> type = makeDecimalType(width, scale, isSigned);
                CPPUNIT_ASSERT(mapper.mapField("amount", type, warnings).duckdbType == duckdb::LogicalType::DECIMAL(width, scale));
            }
        }
        Owned<ITypeInfo> fractionalType = makeDecimalType(12, 4, true);
        CPPUNIT_ASSERT(mapper.mapField("fractional", fractionalType, warnings).duckdbType == duckdb::LogicalType::DECIMAL(12, 4));
        const RtlDecimalTypeInfo runtimeType(type_decimal | type_unsigned, 38 | (6U << 16));
        const RtlFieldInfo field("largest", nullptr, &runtimeType);
        CPPUNIT_ASSERT(mapRtlField(field, nullptr).duckdbType == duckdb::LogicalType::DECIMAL(38, 6));
        CPPUNIT_ASSERT_THROW(mapDecimalType(0, 0), duckdb::BinderException);
        CPPUNIT_ASSERT_THROW(mapDecimalType(39, 2), duckdb::BinderException);
        CPPUNIT_ASSERT_THROW(mapDecimalType(38, 39), duckdb::BinderException);
        CPPUNIT_ASSERT(warnings.empty());
    }

    void testNestedTypes()
    {
        TypeMapper mapper;
        std::vector<Warning> warnings;
        Owned<ITypeInfo> integerType = makeIntType(8, false);
        Owned<ITypeInfo> decimalType = makeDecimalType(12, 4, true);
        Owned<IDefRecordBuilder> childBuilder = createDErecord(0);
        childBuilder->addChildOwn(createDEfield(createAtom("id"), integerType, nullptr));
        childBuilder->addChildOwn(createDEfield(createAtom("amount"), decimalType, nullptr));
        Owned<IDefRecordElement> childRecord = childBuilder->close();
        Owned<ITypeInfo> recordType = makeRecordType();
        Owned<ITypeInfo> rowType = makeRowType(LINK(recordType));
        Owned<ITypeInfo> tableType = makeTableType(LINK(rowType));
        Owned<ITypeInfo> dictionaryType = makeDictionaryType(LINK(rowType));
        Owned<IDefRecordBuilder> builder = createDErecord(0);
        builder->addChildOwn(createDEfield(createAtom("row"), rowType, childRecord));
        builder->addChildOwn(createDEfield(createAtom("rows"), tableType, childRecord));
        builder->addChildOwn(createDEfield(createAtom("dictionary"), dictionaryType, childRecord));
        Owned<IDefRecordElement> record = builder->close();
        std::vector<FieldDescriptor> fields;
        mapper.mapRecord(record, nullptr, fields, warnings);
        const duckdb::LogicalType expectedRow = duckdb::LogicalType::STRUCT({
            { "id", duckdb::LogicalType::UBIGINT },{ "amount", duckdb::LogicalType::DECIMAL(12, 4) } });
        const duckdb::LogicalType expectedWrapper = duckdb::LogicalType::STRUCT({ { "field", expectedRow } });
        CPPUNIT_ASSERT_EQUAL(size_t{ 3 }, fields.size());
        CPPUNIT_ASSERT(fields[0].duckdbType == expectedWrapper);
        CPPUNIT_ASSERT(fields[1].duckdbType == duckdb::LogicalType::LIST(expectedRow));
        CPPUNIT_ASSERT(fields[2].duckdbType == duckdb::LogicalType::LIST(expectedWrapper));
        CPPUNIT_ASSERT_EQUAL(std::string("row.field.amount"), fields[0].children[0].children[1].fieldPath);
        const std::vector<FieldDescriptor> singleChild = { mapper.mapField("items.id", integerType, warnings) };
        CPPUNIT_ASSERT(mapNestedType(type_set, singleChild) == duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT));
        CPPUNIT_ASSERT(mapNestedType(type_dictionary, singleChild) == duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT({ { "id", duckdb::LogicalType::UBIGINT } })));
        CPPUNIT_ASSERT(mapNestedType(type_table, {}) == duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));
        CPPUNIT_ASSERT(warnings.empty());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(WsSql2TypeMappingTests);
#endif // _USE_CPPUNIT

}