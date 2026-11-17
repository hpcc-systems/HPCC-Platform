#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "extension.hpp"
#include "typemapping.hpp"

#ifdef _DEBUG
#include "duckdb/main/client_context.hpp"
#endif

#include "fileview.hpp"
#include "jlib.hpp"
#include "thorcommon.hpp"
#include "thorread.hpp"

namespace duckdb::hpcc
{

#ifdef _DEBUG
// Counts one scan object against the ScanObjectCounters of the connection that created it, from track() until the
// object is destroyed.
class CScanObjectTracker
{
public:
    CScanObjectTracker() = default;
    CScanObjectTracker(const CScanObjectTracker&) = delete;
    CScanObjectTracker& operator=(const CScanObjectTracker&) = delete;
    ~CScanObjectTracker()
    {
        if (counters)
            --counters->live[kind];
    }

    void track(duckdb::ClientContext& context, ScanObjectKind _kind)
    {
        if (counters)
            return;
        auto provider = context.registered_state->Get<ScanMetadataProvider>(ScanMetadataProvider::stateKey);
        if (!provider || !provider->scanObjectCounters)
            return;
        counters = provider->scanObjectCounters;
        kind = static_cast<unsigned>(_kind);
        ++counters->created[kind];
        ++counters->live[kind];
    }

private:
    std::shared_ptr<ScanObjectCounters> counters;
    unsigned kind = 0;
};
#endif

class CScanBindData : public duckdb::TableFunctionData
{
public:
    struct ExpectedRecord final
    {
        ExpectedRecord(const RtlFieldInfo* const* fields, bool expandFields) : record(fields, expandFields) {}

        RtlRecord record;
    };

    CScanBindData(std::string _logicalName, IOutputMetaData* _expectedMetadata, IOutputMetaData* _actualMetadata, std::vector<FieldDescriptor> _fields) : logicalName(std::move(_logicalName)), actualMetadata(LINK(_actualMetadata)), expectedMetadata(LINK(_expectedMetadata)), fields(std::move(_fields))
    {
        expectedSourceMetadata.set(_expectedMetadata);
        if (expectedMetadata)
        {
            const RtlRecord& expectedLayoutRecord = expectedMetadata->queryRecordAccessor(true);
            for (const auto& field : fields)
            {
                const RtlFieldInfo* expectedField = expectedLayoutRecord.queryOriginalField(field.fieldPath.c_str());
                if (!expectedField)
                {
                    expectedFields.clear();
                    break;
                }
                expectedFields.push_back(expectedField);
            }
            if (!expectedFields.empty())
            {
                expectedFields.push_back(nullptr);
                expectedType = std::make_unique<RtlRecordTypeInfo>(type_record, 0, expectedFields.data());
                expectedMetadata.setown(new CDynamicOutputMetaData(*expectedType));
                expectedRecord = std::make_unique<ExpectedRecord>(expectedFields.data(), true);
            }
        }
    }

    std::string logicalName;
    Owned<IOutputMetaData> actualMetadata;
    Owned<IOutputMetaData> expectedSourceMetadata;
    std::vector<const RtlFieldInfo*> expectedFields;
    std::unique_ptr<RtlRecordTypeInfo> expectedType;
    Owned<IOutputMetaData> expectedMetadata;
    std::unique_ptr<ExpectedRecord> expectedRecord;
    std::vector<FieldDescriptor> fields;
    FieldFilterArray pushedFilters;
    std::vector<std::string> keyedFilterDescriptions;
    std::vector<std::string> postFilterDescriptions;
    std::vector<std::string> residualFilterDescriptions;
    std::vector<std::string> readerProjectionDescriptions;
    bool filterLayoutTranslated = false;
    unsigned scanParts = 1;
    std::optional<uint64_t> recordCount;
    int64_t limit = -1;
    int64_t offset = 0;
    bool isKey = false;
    std::vector<std::vector<RemoteFilename>> offlinePartCopies;
    std::vector<unsigned> offlinePartCrcs;
    std::vector<bool> offlineTopLevelKeyParts;
    FileAccessOptions offlineAccessOptions;
#ifdef _DEBUG
    CScanObjectTracker scanObjectTracker;
#endif
};

inline const RtlTypeInfo* queryFilterValueType(const RtlTypeInfo* type)
{
    if (type && type->getType() == type_keyedint && type->queryChildType())
        return type->queryChildType();
    return type;
}

} // namespace duckdb::hpcc
