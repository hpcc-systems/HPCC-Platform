#pragma once

#include <memory>

#include "hpcc-scan.hpp"

namespace duckdb::hpcc
{

FieldDescriptor mapRtlField(const RtlFieldInfo& field, const char* prefix);
std::vector<FieldDescriptor> mapRtlRecord(const RtlRecord& record);
std::vector<FieldDescriptor> appendOfflineScanColumns(IOutputMetaData& layoutMetadata, const std::vector<FieldDescriptor>& descriptorFields, duckdb::vector<duckdb::LogicalType>& returnTypes, duckdb::vector<std::string>& names);

class CScanMaterializer
{
public:
    CScanMaterializer(duckdb::DataChunk& output, const std::vector<FieldDescriptor>& projectedDescriptors, IBlobProvider* blobProvider, bool hasNestedProjection);
    ~CScanMaterializer();

    void materialize(const RtlRecord& record, const RtlRow& row, const byte* rawRow, const std::vector<unsigned>& projectedColumns, duckdb::idx_t rowIndex, byte* conditions);

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace duckdb::hpcc
