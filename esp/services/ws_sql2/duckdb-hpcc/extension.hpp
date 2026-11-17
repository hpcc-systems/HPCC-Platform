#pragma once

#include "duckdb/main/extension.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/function/table_function.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#ifdef _DEBUG
#include <atomic>
#endif

// Opaque here so the public contract does not depend on jlib headers; include jptree.hpp to build or read the trees.
struct IPropertyTree;
class IOutputMetaData;

namespace duckdb::hpcc
{

#ifdef _DEBUG
enum class ScanObjectKind : unsigned
{
    BindData,
    GlobalState,
    LocalState,
    Max
};

// Debug builds only: created and live counts of the hpcc_scan objects (bind data and global/local scan states) that
// the scans bound through one connection create. A host that keeps a reference can confirm, after the connection is
// destroyed, that every object was destroyed with it.
struct ScanObjectCounters
{
    std::atomic<uint64_t> created[static_cast<unsigned>(ScanObjectKind::Max)] {};
    std::atomic<uint64_t> live[static_cast<unsigned>(ScanObjectKind::Max)] {};
};
#endif

struct ScanPart
{
    std::vector<std::string> copies; // Physical paths that can be attempted for this part.
    std::optional<unsigned> crc;
    bool topLevelKeyPart = false;
};

// Mirrors RecordTranslationMode (rtldynfield.hpp); extension.cpp checks that the values stay in step.
enum class ScanTranslationMode : unsigned char
{
    None = 0,
    Payload = 2,
    AlwaysDisk = 3,
    AlwaysECL = 4,
    PayloadRemoveOnly = 5,
    Unspecified = 6
};

struct ScanPropertyTreeRelease
{
    void operator()(IPropertyTree* tree) const;
};

// Takes ownership of one reference to the tree.
using ScanPropertyTree = std::unique_ptr<IPropertyTree, ScanPropertyTreeRelease>;

// Defaults apply to both typed providers and the JSON long form; empty strings are normalized to these at bind time.
struct ScanOptions
{
    static constexpr const char* defaultLogicalName = "offline";
    static constexpr const char* defaultKind = "flat";
    static constexpr const char* defaultFormat = "flat";

    std::string logicalName = defaultLogicalName;
    std::string kind = defaultKind;
    std::string format = defaultFormat;
    bool grouped = false;
    unsigned formatCrc = 0;
    ScanTranslationMode recordTranslationMode = ScanTranslationMode::Unspecified;
    int64_t modified = 0;
    ScanPropertyTree formatOptions;
    ScanPropertyTree providerOptions;
};

// Move-only because the option trees are owned; use copyTo() for an independent deep copy.
class ScanMetadata
{
public:
    // Version of the options JSON contract. Options JSON may omit @version; any other version is rejected.
    static constexpr unsigned optionsVersion = 1;

    std::string recordDefinition;
    std::vector<ScanPart> parts;
    ScanOptions options;
    std::optional<uint64_t> recordCount;

    void deserializeLongForm(const std::string& recordDefinition, const std::string& partsJson, const std::string& optionsJson);
    void copyTo(ScanMetadata& target) const;
    std::string serializePartsJson() const;
    std::string serializeOptionsJson() const;
};

bool hasUnsupportedRecordLayout(const IOutputMetaData& metadata);

// Registered per connection by the host. hpcc_scan(logicalName) resolves logical files through
// resolveScanMetadata(), which must enforce the requesting user's file permissions.
// hpcc_scan(record, parts, options) opens caller-supplied physical paths without any authorization. The host
// decides whether it is available through allowsLongFormScan(); it is always available when no provider is
// registered. WsSQL2 disables it on its shared service path.
class ScanMetadataProvider : public duckdb::ClientContextState
{
public:
    static constexpr const char* stateKey = "ws_sql2.scan_metadata_provider";
    virtual void resolveScanMetadata(const std::string& logicalName, ScanMetadata& metadata) = 0;
    virtual bool allowsLongFormScan() const = 0;
#ifdef _DEBUG
    // Set by the host to count the scan objects created through this connection; scans are not counted when null.
    std::shared_ptr<ScanObjectCounters> scanObjectCounters;
#endif
};

class Extension : public duckdb::Extension
{
public:
    void Load(duckdb::ExtensionLoader& loader) override;
    std::string Name() override;
    std::string Version() const override;
};

} // namespace duckdb::hpcc
