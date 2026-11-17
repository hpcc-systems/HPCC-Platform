#pragma once

#include "hpcc-scan.hpp"

namespace duckdb::hpcc
{

void configureHpccScanOptimizer(duckdb::ExtensionLoader& loader, duckdb::TableFunction& function);

bool supportsHpccScanPushdownType(const duckdb::FunctionData& functionData, duckdb::idx_t fieldIndex);
bool buildHpccScanTableFilter(const duckdb::TableFilter& tableFilter, unsigned fieldIndex, const RtlRecord& record, FieldFilterArray& staged);
// Replaces range filters on the same field with their intersection, so a KEY can use a single keyed filter for them.
void mergeHpccScanFieldFilters(FieldFilterArray& filters);
std::string describeHpccScanFieldFilter(const IFieldFilter& filter, const std::string& fieldName);
std::string joinHpccScanDescriptions(const std::vector<std::string>& descriptions);

} // namespace duckdb::hpcc
