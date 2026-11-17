#pragma once

#include <string>
#include <vector>

#include "duckdb/common/types.hpp"
#include "deffield.hpp"

namespace duckdb::hpcc
{

struct Warning
{
    std::string code;
    std::string fieldPath;
    std::string message;
};

struct FieldDescriptor
{
    std::string fieldPath;
    duckdb::LogicalType duckdbType = duckdb::LogicalType::VARCHAR;
    bool fallback = false;
    std::vector<FieldDescriptor> children;
};

duckdb::LogicalType mapDecimalType(unsigned width, unsigned scale);
void mapScalarFieldType(FieldDescriptor& descriptor, type_t sourceType, bool unsigned64, unsigned decimalDigits, unsigned decimalPrecision);
duckdb::LogicalType mapNestedType(type_t sourceType, const std::vector<FieldDescriptor>& children);

class TypeMapper
{
public:
    FieldDescriptor mapField(const char* fieldPath, ITypeInfo* sourceType, std::vector<Warning>& warnings) const;
    void mapRecord(const IDefRecordElement* record, const char* prefix, std::vector<FieldDescriptor>& fields, std::vector<Warning>& warnings) const;
};

} // namespace duckdb::hpcc
