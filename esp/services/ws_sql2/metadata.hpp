#ifndef _ESP_WS_SQL2_METADATA_HPP_
#define _ESP_WS_SQL2_METADATA_HPP_

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "fileview.hpp"
#include "rtldynfield.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace duckdb::hpcc
{
class ScanMetadata;
}

IOutputMetaData* createWsSql2OutputMetaDataFromEcl(const char* ecl, const char* kind, bool grouped);

struct CScanDescriptor
{
    std::string logicalName;
    std::string parts;
    std::string options;
    std::string sqlSchema;
    std::string eclRecordDefinition;
    std::string sampleSql;
    std::optional<uint64_t> recordCount;
};

class CMetadataResolver
{
    static bool parseLogicalName(const char* logicalName, CDfsLogicalFileName& parsedName);
    static bool isExternalOrForeignName(const CDfsLogicalFileName& parsedName);

public:
    static bool isValidLogicalName(const char* logicalName);
    static Owned<IDistributedFile> resolveLogicalFile(const char* logicalName, IUserDescriptor* user);
    static void buildScanDescriptor(IDistributedFile& file, CScanDescriptor& descriptor, const char* recordDefinitionOverride);
    static void buildScanMetadata(IDistributedFile& file, duckdb::hpcc::ScanMetadata& metadata, const char* recordDefinitionOverride);
};

#endif