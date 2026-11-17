#include "extension.hpp"

#include "materialization.hpp"
#include "optimizer.hpp"

#include "fileview.hpp"
#include "jlib.hpp"
#include "jptree.hpp"
#include "thorcommon.hpp"
#include "thorfile.hpp"
#include "thorread.hpp"
#include "jhtree.hpp"
#include "rtldynfield.hpp"

#include <atomic>
#include <algorithm>
#include <unordered_set>

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb::hpcc
{
static_assert(static_cast<unsigned>(ScanTranslationMode::None) == static_cast<unsigned>(RecordTranslationMode::None));
static_assert(static_cast<unsigned>(ScanTranslationMode::Payload) == static_cast<unsigned>(RecordTranslationMode::Payload));
static_assert(static_cast<unsigned>(ScanTranslationMode::AlwaysDisk) == static_cast<unsigned>(RecordTranslationMode::AlwaysDisk));
static_assert(static_cast<unsigned>(ScanTranslationMode::AlwaysECL) == static_cast<unsigned>(RecordTranslationMode::AlwaysECL));
static_assert(static_cast<unsigned>(ScanTranslationMode::PayloadRemoveOnly) == static_cast<unsigned>(RecordTranslationMode::PayloadRemoveOnly));
static_assert(static_cast<unsigned>(ScanTranslationMode::Unspecified) == static_cast<unsigned>(RecordTranslationMode::Unspecified));

void ScanPropertyTreeRelease::operator()(IPropertyTree* tree) const
{
    ::Release(tree);
}

static const char* queryTextOrDefault(const char* value, const char* defaultValue)
{
    return isEmptyString(value) ? defaultValue : value;
}

static bool hasUnsupportedIfBlocks(const RtlTypeInfo& type)
{
    // Serial IFBLOCKs can evaluate their serialized field filters; complex blocks need generated code.
    if (type.getType() == type_ifblock && !dynamic_cast<const RtlSerialIfBlockTypeInfo*>(&type))
        return true;

    const RtlFieldInfo * const * fields = type.queryFields();
    for (; fields && *fields; ++fields)
    {
        if (hasUnsupportedIfBlocks(*(*fields)->type))
            return true;
    }
    return false;
}

bool hasUnsupportedRecordLayout(const IOutputMetaData& metadata)
{
    const RtlTypeInfo* type = metadata.queryTypeInfo();
    return type ? hasUnsupportedIfBlocks(*type) : metadata.queryRecordAccessor(true).getNumIfBlocks() != 0;
}

static bool isValidTranslationMode(__int64 value)
{
    switch (value)
    {
    case static_cast<__int64>(ScanTranslationMode::None):
    case static_cast<__int64>(ScanTranslationMode::Payload):
    case static_cast<__int64>(ScanTranslationMode::AlwaysDisk):
    case static_cast<__int64>(ScanTranslationMode::AlwaysECL):
    case static_cast<__int64>(ScanTranslationMode::PayloadRemoveOnly):
    case static_cast<__int64>(ScanTranslationMode::Unspecified):
        return true;
    default:
        return false;
    }
}

// Returns a copy of an optional nested options object, rejecting scalars and arrays.
static IPropertyTree* getOptionsObject(const IPropertyTree& optionsTree, const char* name)
{
    if (!optionsTree.hasProp(name))
        return nullptr;
    if (optionsTree.getCount(name) != 1)
        throw duckdb::BinderException("hpcc_scan offline overload requires options %s to be a JSON object", name);
    const IPropertyTree* child = optionsTree.queryPropTree(name);
    if (!child || child->queryProp(nullptr))
        throw duckdb::BinderException("hpcc_scan offline overload requires options %s to be a JSON object", name);
    return createPTreeFromIPT(child);
}

void ScanMetadata::deserializeLongForm(const std::string& _recordDefinition, const std::string& partsJson, const std::string& optionsJson)
{
    if (_recordDefinition.empty() || partsJson.empty() || optionsJson.empty())
        throw duckdb::BinderException("hpcc_scan offline overload requires ECL record, parts JSON, and options JSON");
    if (_recordDefinition.size() > 0x100000)
        throw duckdb::BinderException("hpcc_scan offline overload received an oversized record definition");

    Owned<IPropertyTree> optionsTree = createPTreeFromJSONString(optionsJson.c_str());
    if (!optionsTree)
        throw duckdb::BinderException("hpcc_scan offline overload received invalid options JSON");
    const __int64 version = optionsTree->getPropInt64("@version", optionsVersion);
    if (version != optionsVersion)
        throw duckdb::BinderException("hpcc_scan offline overload received unsupported options version %lld (supported %u)", static_cast<long long>(version), optionsVersion);
    const __int64 translationMode = optionsTree->getPropInt64("@recordTranslationMode", static_cast<__int64>(ScanTranslationMode::Unspecified));
    if (!isValidTranslationMode(translationMode))
        throw duckdb::BinderException("hpcc_scan offline overload received invalid recordTranslationMode %lld", static_cast<long long>(translationMode));
    Owned<IPropertyTree> partsTree = createPTreeFromJSONString(partsJson.c_str());
    if (!partsTree)
        throw duckdb::BinderException("hpcc_scan offline overload received invalid parts JSON");

    // Parse everything before assigning so a rejected payload leaves this object unchanged.
    std::vector<ScanPart> parsedParts;
    Owned<IPropertyTreeIterator> partIterator = partsTree->getElements("part");
    ForEach(*partIterator)
    {
        IPropertyTree& partTree = partIterator->query();
        ScanPart part;
        if (partTree.hasProp("@crc"))
        {
            const __int64 crc = partTree.getPropInt64("@crc", -1);
            if (crc < 0 || crc > static_cast<__int64>(UINT_MAX))
                throw duckdb::BinderException("hpcc_scan offline overload received part @crc outside the unsigned 32-bit range");
            part.crc = static_cast<unsigned>(crc);
        }
        part.topLevelKeyPart = partTree.getPropBool("@tlk", false);
        Owned<IPropertyTreeIterator> copyIterator = partTree.getElements("copy");
        ForEach(*copyIterator)
        {
            const char* path = copyIterator->query().queryProp(".");
            if (!isEmptyString(path))
                part.copies.emplace_back(path);
        }
        if (!part.copies.empty())
            parsedParts.emplace_back(std::move(part));
    }
    ScanPropertyTree formatOptions(getOptionsObject(*optionsTree, "formatOptions"));
    ScanPropertyTree providerOptions(getOptionsObject(*optionsTree, "providerOptions"));

    recordDefinition = _recordDefinition;
    parts = std::move(parsedParts);
    options.logicalName = queryTextOrDefault(optionsTree->queryProp("@logicalName"), ScanOptions::defaultLogicalName);
    options.kind = queryTextOrDefault(optionsTree->queryProp("@kind"), ScanOptions::defaultKind);
    options.format = queryTextOrDefault(optionsTree->queryProp("@format"), ScanOptions::defaultFormat);
    options.grouped = optionsTree->getPropBool("@grouped", false);
    options.formatCrc = optionsTree->getPropInt("@formatCrc", 0);
    options.recordTranslationMode = static_cast<ScanTranslationMode>(translationMode);
    options.modified = optionsTree->getPropInt64("@modified", 0);
    options.formatOptions = std::move(formatOptions);
    options.providerOptions = std::move(providerOptions);
    recordCount.reset();
}

static ScanPropertyTree cloneScanPropertyTree(const ScanPropertyTree& tree)
{
    return ScanPropertyTree(tree ? createPTreeFromIPT(tree.get()) : nullptr);
}

void ScanMetadata::copyTo(ScanMetadata& target) const
{
    target.recordDefinition = recordDefinition;
    target.parts = parts;
    target.options.logicalName = options.logicalName;
    target.options.kind = options.kind;
    target.options.format = options.format;
    target.options.grouped = options.grouped;
    target.options.formatCrc = options.formatCrc;
    target.options.recordTranslationMode = options.recordTranslationMode;
    target.options.modified = options.modified;
    target.options.formatOptions = cloneScanPropertyTree(options.formatOptions);
    target.options.providerOptions = cloneScanPropertyTree(options.providerOptions);
    target.recordCount = recordCount;
}

std::string ScanMetadata::serializePartsJson() const
{
    Owned<IPropertyTree> partsTree = createPTree("parts");
    for (const ScanPart& part : parts)
    {
        IPropertyTree* partTree = partsTree->addPropTree("part", createPTree("part"));
        partTree->setPropBool("@tlk", part.topLevelKeyPart);
        if (part.crc)
            partTree->setPropInt64("@crc", *part.crc);
        for (const std::string& copy : part.copies)
            partTree->addProp("copy", copy.c_str());
    }
    StringBuffer json;
    toJSON(partsTree, json, 0, 0);
    return json.str();
}

std::string ScanMetadata::serializeOptionsJson() const
{
    Owned<IPropertyTree> optionsTree = createPTree("options");
    optionsTree->setPropInt("@version", optionsVersion);
    optionsTree->setProp("@logicalName", options.logicalName.c_str());
    optionsTree->setProp("@kind", options.kind.c_str());
    optionsTree->setProp("@format", options.format.c_str());
    optionsTree->setPropBool("@grouped", options.grouped);
    optionsTree->setPropInt("@formatCrc", options.formatCrc);
    optionsTree->setPropInt("@recordTranslationMode", static_cast<unsigned>(options.recordTranslationMode));
    optionsTree->setPropInt64("@modified", options.modified);
    if (options.formatOptions)
        optionsTree->setPropTree("formatOptions", createPTreeFromIPT(options.formatOptions.get()));
    Owned<IPropertyTree> providerOptions;
    if (options.providerOptions)
        providerOptions.setown(createPTreeFromIPT(options.providerOptions.get()));
    else
        providerOptions.setown(createPTree("providerOptions"));
    if (!providerOptions->hasProp("@compressed"))
        providerOptions->setPropBool("@compressed", false);
    optionsTree->setPropTree("providerOptions", providerOptions.getClear());
    StringBuffer json;
    toJSON(optionsTree, json, 0, 0);
    return json.str();
}

namespace
{
static IOutputMetaData* createHpccScanOutputMetaDataFromEcl(const char* ecl, const char* kind, bool grouped)
{
    if (isEmptyString(ecl))
        return nullptr;
    Owned<IPropertyTree> attributes = createPTree("Attr");
    attributes->setProp("ECL", ecl);
    if (!isEmptyString(kind))
        attributes->setProp("@kind", kind);
    attributes->setPropBool("@grouped", grouped);
    return getDaliLayoutInfo(*attributes);
}

struct COfflineScanData
{
    std::string logicalName;
    bool isKey = false;
    Owned<IOutputMetaData> metadata;
    Owned<IOutputMetaData> expectedMetadata;
    FileAccessOptions accessOptions;
    std::vector<FieldDescriptor> fields;
    std::vector<std::vector<RemoteFilename>> partCopies;
    std::vector<unsigned> partCrcs;
    std::vector<bool> topLevelKeyParts;
};

static void parseOfflineScanData(const ScanMetadata& scanMetadata, COfflineScanData& data)
{
    data.logicalName = queryTextOrDefault(scanMetadata.options.logicalName.c_str(), ScanOptions::defaultLogicalName);
    if (scanMetadata.recordDefinition.empty())
        throw duckdb::BinderException("hpcc_scan metadata for '%s' has no ECL record definition", data.logicalName);
    const char* kind = queryTextOrDefault(scanMetadata.options.kind.c_str(), ScanOptions::defaultKind);
    data.isKey = strieq(kind, "key");
    data.accessOptions.format.set(queryTextOrDefault(scanMetadata.options.format.c_str(), ScanOptions::defaultFormat));
    data.accessOptions.formatCrc = scanMetadata.options.formatCrc;
    data.accessOptions.recordTranslationMode = static_cast<RecordTranslationMode>(scanMetadata.options.recordTranslationMode);
    if (scanMetadata.options.formatOptions)
        data.accessOptions.formatOptions.setown(createPTreeFromIPT(scanMetadata.options.formatOptions.get()));
    if (scanMetadata.options.providerOptions)
        data.accessOptions.providerOptions.setown(createPTreeFromIPT(scanMetadata.options.providerOptions.get()));

    data.metadata.setown(createHpccScanOutputMetaDataFromEcl(scanMetadata.recordDefinition.c_str(), kind, scanMetadata.options.grouped));
    if (!data.metadata || !data.metadata->queryTypeInfo() || data.metadata->queryTypeInfo()->getType() != type_record)
        throw duckdb::BinderException("hpcc_scan offline overload received an invalid ECL record definition or unsupported record layout");
    if (hasUnsupportedRecordLayout(*data.metadata))
        throw duckdb::BinderException("hpcc_scan encountered an unsupported record layout that requires a compiled record helper");
    data.accessOptions.actualDiskMeta.set(data.metadata);
    data.expectedMetadata.set(data.metadata);
    const RtlRecord& record = data.metadata->queryRecordAccessor(true);
    data.fields = mapRtlRecord(record);

    for (const ScanPart& part : scanMetadata.parts)
    {
        std::vector<RemoteFilename> copies;
        for (const std::string& path : part.copies)
        {
            RemoteFilename filename;
            filename.setRemotePath(path.c_str());
            if (!filename.isNull())
                copies.push_back(filename);
        }
        if (!copies.empty())
        {
            data.partCopies.push_back(std::move(copies));
            data.partCrcs.push_back(part.crc.value_or(0));
            data.topLevelKeyParts.push_back(part.topLevelKeyPart);
        }
    }
    if (data.partCopies.empty())
        throw duckdb::BinderException("hpcc_scan offline overload received no readable parts");
}

// The key cache is keyed by filename and CRC, so pass the part CRC to avoid reusing a stale cached index
// when a key part is replaced at the same path.
static IKeyIndex* openOfflineKeyFile(const std::vector<RemoteFilename>& copies, unsigned crc)
{
    Owned<IException> lastException;
    for (const auto& filename : copies)
    {
        try
        {
            StringBuffer path;
            filename.getPath(path);
            OwnedIFile file = createIFile(path.str());
            Owned<IFileIO> fileIO = file->open(IFOread);
            if (fileIO)
                return createKeyIndex(path.str(), crc, *fileIO, static_cast<unsigned>(-1), false, {});
        }
        catch (IException* exception)
        {
            lastException.setown(exception);
        }
    }
    if (lastException)
    {
        StringBuffer message;
        lastException->errorMessage(message);
        throw duckdb::IOException("hpcc_scan could not open offline KEY part: %s", message.str());
    }
    throw duckdb::IOException("hpcc_scan could not open offline KEY part");
}

class CScanGlobalState : public duckdb::GlobalTableFunctionState
{
public:
    FileAccessOptions accessOptions;
    Owned<IOutputMetaData> actualMetadata;
    std::vector<const RtlFieldInfo*> projectedFields;
    std::unique_ptr<RtlRecordTypeInfo> projectedType;
    Owned<IOutputMetaData> projectedMetadata;
    Owned<IRowReadFormatMapping> mapping;
    std::vector<std::vector<RemoteFilename>> partCopies;
    std::atomic<duckdb::idx_t> nextPart{ 0 };
    bool isKey = false;
    IArrayOf<IKeyIndex> keyParts;
    RowFilter keyedFilters;
    FieldFilterArray filters;
    RowFilter postFilters;
    std::vector<std::string> keyedFilterDescriptions;
    std::vector<std::string> postFilterDescriptions;
    std::vector<unsigned> projectedColumns;
    std::vector<FieldDescriptor> projectedDescriptors;
    bool hasNestedProjection = false;
    int64_t limit = -1;
    int64_t offset = 0;
    int64_t rowsReturned = 0;
    int64_t rowsSkipped = 0;
#ifdef _DEBUG
    CScanObjectTracker scanObjectTracker;
#endif

    idx_t MaxThreads() const override
    {
        return std::max<idx_t>(isKey ? keyParts.ordinality() : partCopies.size(), 1);
    }
};

class CScanLocalState : public duckdb::LocalTableFunctionState
{
public:
    Owned<IDiskRowReader> reader;
    Linked<ILogicalRowStream> stream;
    Owned<IKeyManager> keyManager;
    Owned<IIndexLookup> keyLookup;
    std::vector<size_t> postFilterVariableOffsets;
    std::unique_ptr<RtlRow> postFilterRow;
    duckdb::idx_t partIndex = 0;
    int64_t rowsReturned = 0;
    int64_t rowsSkipped = 0;
#ifdef _DEBUG
    CScanObjectTracker scanObjectTracker;
#endif
};

static bool openNextHpccScanKeyPart(CScanGlobalState& global, CScanLocalState& local, const char* logicalName)
{
    local.keyLookup.clear();
    local.keyManager.clear();
    const duckdb::idx_t partIndex = global.nextPart.fetch_add(1);
    if (partIndex >= global.keyParts.ordinality())
        return false;
    try
    {
        local.keyManager.setown(createLocalKeyManager(global.actualMetadata->queryRecordAccessor(true), &global.keyParts.item(static_cast<unsigned>(partIndex)), nullptr, true, false));
        global.keyedFilters.createSegmentMonitors(local.keyManager);
        local.keyManager->finishSegmentMonitors();
        local.keyManager->reset();
        local.keyLookup.setown(createIndexLookup(local.keyManager));
        local.keyLookup->ensureAvailable();
    }
    catch (IException* exception)
    {
        StringBuffer message;
        exception->errorMessage(message);
        exception->Release();
        throw duckdb::IOException("hpcc_scan could not open KEY part %llu of %s: %s", static_cast<unsigned long long>(partIndex + 1), logicalName, message.str());
    }
    local.partIndex = partIndex;
    return true;
}

static bool openNextHpccScanPart(CScanGlobalState& global, CScanLocalState& local, const char* logicalName)
{
    local.stream.clear();
    const duckdb::idx_t partIndex = global.nextPart.fetch_add(1);
    if (partIndex >= global.partCopies.size())
        return false;
    // Try each copy of the part in order so that an unavailable primary fails over to a replica.
    StringBuffer lastError;
    bool opened = false;
    for (const RemoteFilename& filename : global.partCopies[partIndex])
    {
        try
        {
            if (!local.reader)
                local.reader.setown(createDiskReader(global.accessOptions.format, false, global.mapping, global.accessOptions.providerOptions, nullptr));
            else
                local.reader->clearInput();
            if (local.reader->setInputFile(filename, logicalName, 0, 0, global.filters))
            {
                opened = true;
                break;
            }
            lastError.set("file could not be opened");
        }
        catch (IException* exception)
        {
            lastError.clear();
            exception->errorMessage(lastError);
            exception->Release();
        }
    }
    if (!opened)
        throw duckdb::IOException("hpcc_scan could not open part %llu of %s: %s", static_cast<unsigned long long>(partIndex + 1), logicalName, lastError.str());
    local.stream.set(local.reader->queryAllocatedRowStream());
    local.partIndex = partIndex;
    return true;
}

static duckdb::unique_ptr<duckdb::FunctionData> bindHpccScanLongForm(const ScanMetadata& scanMetadata, duckdb::vector<duckdb::LogicalType>& returnTypes, duckdb::vector<std::string>& names)
{
    COfflineScanData offlineData;
    parseOfflineScanData(scanMetadata, offlineData);
    IOutputMetaData* actualMetadata = offlineData.metadata.get();
    auto fields = appendOfflineScanColumns(*actualMetadata, offlineData.fields, returnTypes, names);
    unsigned scanParts = 0;
    for (unsigned partIndex = 0; partIndex < offlineData.partCopies.size(); ++partIndex)
    {
        if (!offlineData.isKey || !offlineData.topLevelKeyParts[partIndex])
            ++scanParts;
    }
    auto bindData = duckdb::make_uniq<CScanBindData>(offlineData.logicalName, offlineData.expectedMetadata.get(), actualMetadata, std::move(fields));
    bindData->isKey = offlineData.isKey;
    bindData->scanParts = scanParts;
    bindData->offlinePartCopies = std::move(offlineData.partCopies);
    bindData->offlinePartCrcs = std::move(offlineData.partCrcs);
    bindData->offlineTopLevelKeyParts = std::move(offlineData.topLevelKeyParts);
    bindData->offlineAccessOptions.format.set(offlineData.accessOptions.format);
    bindData->offlineAccessOptions.recordTranslationMode = offlineData.accessOptions.recordTranslationMode;
    bindData->offlineAccessOptions.formatCrc = offlineData.accessOptions.formatCrc;
    bindData->offlineAccessOptions.formatOptions.setown(createPTreeFromIPT(offlineData.accessOptions.formatOptions));
    if (offlineData.accessOptions.providerOptions)
        bindData->offlineAccessOptions.providerOptions.setown(createPTreeFromIPT(offlineData.accessOptions.providerOptions));
    bindData->offlineAccessOptions.actualDiskMeta.set(actualMetadata);
    return duckdb::unique_ptr<duckdb::FunctionData>(bindData.release());
}

static duckdb::unique_ptr<duckdb::FunctionData> bindHpccScanUntracked(duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input, duckdb::vector<duckdb::LogicalType>& returnTypes, duckdb::vector<std::string>& names)
{
    auto metadataProvider = context.registered_state->Get<ScanMetadataProvider>(ScanMetadataProvider::stateKey);
    if (input.inputs.size() == 3)
    {
        if (metadataProvider && !metadataProvider->allowsLongFormScan())
            throw duckdb::BinderException("hpcc_scan offline overload is disabled");
        for (const auto& value : input.inputs)
        {
            if (value.IsNull())
                throw duckdb::BinderException("hpcc_scan offline overload requires ECL record, parts JSON, and options JSON");
        }
        ScanMetadata scanMetadata;
        scanMetadata.deserializeLongForm(input.inputs[0].ToString(), input.inputs[1].ToString(), input.inputs[2].ToString());
        return bindHpccScanLongForm(scanMetadata, returnTypes, names);
    }
    if (input.inputs.size() != 1)
        throw duckdb::BinderException("hpcc_scan requires one logical file name or ECL record, parts JSON, and options JSON");

    if (input.inputs[0].IsNull() || input.inputs[0].ToString().empty())
        throw duckdb::BinderException("hpcc_scan requires one logical file name");
    if (!metadataProvider)
        throw duckdb::BinderException("hpcc_scan requires the ws_sql2 metadata provider");

    try
    {
        ScanMetadata metadata;
        metadataProvider->resolveScanMetadata(input.inputs[0].ToString(), metadata);
        auto bindData = bindHpccScanLongForm(metadata, returnTypes, names);
        bindData->Cast<CScanBindData>().recordCount = metadata.recordCount;
        return bindData;
    }
    catch (IException* exception)
    {
        StringBuffer message;
        exception->errorMessage(message);
        exception->Release();
        throw duckdb::BinderException(message.str());
    }
}

duckdb::unique_ptr<duckdb::FunctionData> bindHpccScan(duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input, duckdb::vector<duckdb::LogicalType>& returnTypes, duckdb::vector<std::string>& names)
{
    auto bindData = bindHpccScanUntracked(context, input, returnTypes, names);
#ifdef _DEBUG
    bindData->Cast<CScanBindData>().scanObjectTracker.track(context, ScanObjectKind::BindData);
#endif
    return bindData;
}

void executeHpccScan(duckdb::ClientContext& context, duckdb::TableFunctionInput& input, duckdb::DataChunk& output)
{
    auto& state = input.global_state->Cast<CScanGlobalState>();
    auto& local = input.local_state->Cast<CScanLocalState>();
    const auto& bindData = input.bind_data->Cast<CScanBindData>();
    if (state.limit >= 0 && local.rowsReturned >= state.limit)
    {
        output.SetCardinality(0);
        return;
    }

    // A chunk ends at a part boundary so getHpccScanPartitionData reports the part that produced every row.
    auto nextRow = [&context, &state, &local, &bindData](bool canOpenPart) -> const byte*
        {
            if (state.isKey)
            {
                for (;;)
                {
                    // A selective post-filter can reject many keys before one matches, so honour cancellation here.
                    if (context.IsInterrupted())
                        throw duckdb::InterruptException();
                    if (!local.keyLookup && (!canOpenPart || !openNextHpccScanKeyPart(state, local, bindData.logicalName.c_str())))
                        return nullptr;
                    const byte* row = nullptr;
                    try
                    {
                        row = static_cast<const byte*>(local.keyLookup->nextKey());
                    }
                    catch (IException* exception)
                    {
                        StringBuffer message;
                        exception->errorMessage(message);
                        exception->Release();
                        throw duckdb::IOException("hpcc_scan KEY lookup failed: %s", message.str());
                    }
                    if (!row)
                    {
                        local.keyLookup.clear();
                        continue;
                    }
                    if (!state.postFilters.numFilterFields())
                        return row;

                    if (!local.postFilterRow)
                    {
                        const RtlRecord& record = state.actualMetadata->queryRecordAccessor(true);
                        const unsigned numOffsets = record.getNumVarFields() + 1;
                        local.postFilterVariableOffsets.resize(numOffsets);
                        local.postFilterRow = std::make_unique<RtlRow>(record, nullptr, numOffsets, local.postFilterVariableOffsets.data());
                    }
                    local.postFilterRow->setRow(row, 0);
                    if (state.postFilters.matches(*local.postFilterRow))
                        return row;
                }
            }

            for (;;)
            {
                if (!local.stream && (!canOpenPart || !openNextHpccScanPart(state, local, bindData.logicalName.c_str())))
                    return nullptr;
                size32_t rowSize = 0;
                const byte* row = static_cast<const byte*>(local.stream->prefetchRow(rowSize));
                if (isEndOfFile(row))
                {
                    // XML readers only reset their parser on stop(), so a reused reader would otherwise miss later parts.
                    local.stream->stop();
                    local.stream.clear();
                    continue;
                }
                // SQL rows are ungrouped, so end-of-group markers are dropped.
                if (isSpecialRow(row))
                    continue;
                return row;
            }
        };

    while (local.rowsSkipped < state.offset)
    {
        if (context.IsInterrupted())
            throw duckdb::InterruptException();
        if (!nextRow(true))
        {
            output.SetCardinality(0);
            return;
        } ++local.rowsSkipped;
    }

    const RtlRecord& outputRecord = state.projectedMetadata ? state.projectedMetadata->queryRecordAccessor(true) : state.actualMetadata->queryRecordAccessor(true);

    unsigned numFieldsUsed = 0;
    for (unsigned column : state.projectedColumns)
        numFieldsUsed = std::max(numFieldsUsed, column + 1);
    std::vector<size_t> variableOffsets(outputRecord.getNumVarFields() + 1);
    RtlRow rtlRow(outputRecord, nullptr, static_cast<unsigned>(variableOffsets.size()), variableOffsets.data());
    std::vector<byte> conditions(outputRecord.getNumIfBlocks());
    byte* rowConditions = conditions.empty() ? nullptr : conditions.data();

    class CScanKeyBlobProvider : implements IBlobProvider
    {
    public:
        explicit CScanKeyBlobProvider(CScanLocalState& _local) : local(_local)
        {
        }

        virtual const byte* lookupBlob(unsigned __int64 id) override
        {
            if (!local.keyManager)
                throw duckdb::InternalException("hpcc_scan KEY blob lookup without an open KEY part");
            size32_t size = 0;
            return local.keyManager->loadBlob(id, size, nullptr);
        }

    private:
        CScanLocalState& local;
    } keyBlobProviderInstance(local);
    IBlobProvider* keyBlobProvider = state.isKey ? &keyBlobProviderInstance : nullptr;
    CScanMaterializer materializer(output, state.projectedDescriptors, keyBlobProvider, state.hasNestedProjection);

    duckdb::idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE && (state.limit < 0 || local.rowsReturned < state.limit))
    {
        const byte* row = nextRow(count == 0);
        if (!row)
            break;

        rtlRow.setRow(row, numFieldsUsed);
        if (rowConditions)
            memset(rowConditions, 2, conditions.size()); // 2 = condition not yet evaluated
        materializer.materialize(outputRecord, rtlRow, row, state.projectedColumns, count, rowConditions);
        // Blob values have been copied into the output vectors, so release them per row as before.
        if (local.keyManager)
            local.keyManager->releaseBlobs();
        ++local.rowsReturned;
        ++count;
    }
    output.SetCardinality(count);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> initializeHpccScan([[maybe_unused]] duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input)
{
    const auto& bindData = input.bind_data->Cast<CScanBindData>();
    const bool isKey = bindData.isKey;

    auto state = duckdb::make_uniq<CScanGlobalState>();
#ifdef _DEBUG
    state->scanObjectTracker.track(context, ScanObjectKind::GlobalState);
#endif
    state->isKey = isKey;
    state->limit = bindData.limit;
    state->offset = bindData.offset;
    state->accessOptions = FileAccessOptions(bindData.offlineAccessOptions);
    state->actualMetadata.setown(state->accessOptions.actualDiskMeta.getLink());
    if (!state->actualMetadata)
        state->actualMetadata.setown(bindData.actualMetadata.getLink());
    if (!state->actualMetadata)
        throw duckdb::BinderException("hpcc_scan could not initialize the published record layout: %s has no runtime type information; a compiled record helper may be required", bindData.logicalName);
    if (!isKey && !state->accessOptions.formatCrc)
        throw duckdb::BinderException("hpcc_scan could not initialize the published record layout: " "format '%s' of %s is not supported", state->accessOptions.format.str(), bindData.logicalName);

    if (input.filters)
    {
        const RtlRecord& record = bindData.expectedRecord ? bindData.expectedRecord->record : state->actualMetadata->queryRecordAccessor(true);
        FieldFilterArray stagedFilters;
        for (const auto& entry : input.filters->filters)
        {
            // DuckDB marks advisory filters as optional because query correctness does not depend on them.
            if (entry.second->filter_type == duckdb::TableFilterType::OPTIONAL_FILTER)
                continue;
            if (entry.first >= input.column_ids.size())
                throw duckdb::BinderException("hpcc_scan filter references an unknown projected column");
            const unsigned expectedIndex = static_cast<unsigned>(input.column_ids[entry.first]);
            if (!buildHpccScanTableFilter(*entry.second, expectedIndex, record, stagedFilters))
                throw duckdb::NotImplementedException("hpcc_scan filter cannot be pushed down safely: %s", entry.second->ToString(bindData.fields.at(expectedIndex).fieldPath));
        }
        ForEachItemIn(stagedIndex, stagedFilters)
            state->filters.append(OLINK(stagedFilters.item(stagedIndex)));
    }
    if (!input.projection_ids.empty())
    {
        for (auto projection : input.projection_ids)
            state->projectedColumns.push_back(static_cast<unsigned>(input.column_ids.at(projection)));
    }
    else if (input.column_ids.empty())
    {
        for (unsigned index = 0; index < bindData.fields.size(); ++index)
            state->projectedColumns.push_back(index);
    }
    else
    {
        for (auto column : input.column_ids)
            state->projectedColumns.push_back(static_cast<unsigned>(column));
    }
    for (unsigned projectedColumn : state->projectedColumns)
    {
        state->projectedDescriptors.push_back(bindData.fields.at(projectedColumn));
        const FieldDescriptor& descriptor = state->projectedDescriptors.back();
        const duckdb::LogicalTypeId typeId = descriptor.duckdbType.id();
        if (!descriptor.children.empty() || typeId == duckdb::LogicalTypeId::LIST || typeId == duckdb::LogicalTypeId::STRUCT || typeId == duckdb::LogicalTypeId::MAP)
            state->hasNestedProjection = true;
    }
    ForEachItemIn(pushedIndex, bindData.pushedFilters)
        state->filters.append(OLINK(bindData.pushedFilters.item(pushedIndex)));

    if (!isKey)
    {
        IOutputMetaData* expectedMetadata = bindData.expectedMetadata ? bindData.expectedMetadata.get() : state->actualMetadata.get();
        const RtlRecord& expectedRecord = expectedMetadata->queryRecordAccessor(true);
        // Projecting a field out of its IFBLOCK drops the condition and the translator treats that as unchanged,
        // so read whole rows and test the condition when materializing instead. Fields flattened out of a nested
        // RECORD cannot be matched by name at the top level of the projected record either.
        bool readWholeRows = !bindData.expectedRecord && expectedRecord.getNumIfBlocks() != 0;
        if (!readWholeRows)
        {
            std::unordered_set<const RtlFieldInfo*> topLevelFields;
            for (const RtlFieldInfo* const* field = expectedMetadata->queryTypeInfo()->queryFields(); field && *field; ++field)
                topLevelFields.insert(*field);
            for (unsigned projectedColumn : state->projectedColumns)
            {
                if (projectedColumn < expectedRecord.getNumFields() && !topLevelFields.count(expectedRecord.queryOriginalField(projectedColumn)))
                {
                    readWholeRows = true;
                    break;
                }
            }
        }
        for (unsigned projectedColumn : state->projectedColumns)
        {
            if (projectedColumn >= expectedRecord.getNumFields())
                throw duckdb::BinderException("hpcc_scan projection references an unknown field");
            if (!readWholeRows)
                state->projectedFields.push_back(expectedRecord.queryOriginalField(projectedColumn));
        }
        if (readWholeRows)
            state->projectedMetadata.set(expectedMetadata);
        else
        {
            state->projectedFields.push_back(nullptr);
            state->projectedType = std::make_unique<RtlRecordTypeInfo>(type_record, 0, state->projectedFields.data());
            state->projectedMetadata.setown(new CDynamicOutputMetaData(*state->projectedType));
        }
        const unsigned expectedCrc = state->accessOptions.formatCrc;
        try
        {
            state->mapping.setown(createRowReadFormatMapping(state->accessOptions.recordTranslationMode, state->accessOptions.format, state->accessOptions.formatCrc, *state->actualMetadata, expectedCrc, *expectedMetadata, 0, *state->projectedMetadata, state->accessOptions.formatOptions));
        }
        catch (IException* exception)
        {
            StringBuffer message;
            exception->errorMessage(message);
            exception->Release();
            throw duckdb::IOException("hpcc_scan could not create projected row mapping: %s", message.str());
        }
        if (!readWholeRows)
        {
            state->projectedColumns.clear();
            for (unsigned index = 0; index + 1 < state->projectedFields.size(); ++index)
                state->projectedColumns.push_back(index);
        }
        // Copies are kept in preference order; parseOfflineScanData guarantees at least one per part.
        state->partCopies = bindData.offlinePartCopies;
        ForEachItemIn(describeIndex, state->filters)
        {
            const IFieldFilter& filter = state->filters.item(describeIndex);
            const unsigned fieldIndex = filter.queryFieldIndex();
            const std::string fieldName = fieldIndex < bindData.fields.size() ? bindData.fields[fieldIndex].fieldPath : std::string(expectedRecord.queryName(fieldIndex));
            state->postFilterDescriptions.push_back(describeHpccScanFieldFilter(filter, fieldName));
        }
    }

    if (isKey)
    {
        const RtlRecord& actualRecord = state->actualMetadata->queryRecordAccessor(true);
        std::vector<unsigned> actualColumns(bindData.fields.size(), actualRecord.getNumFields());
        for (unsigned fieldIndex = 0; fieldIndex < bindData.fields.size(); ++fieldIndex)
        {
            const char* fieldPath = bindData.fields[fieldIndex].fieldPath.c_str();
            for (unsigned actualIndex = 0; actualIndex < actualRecord.getNumFields(); ++actualIndex)
            {
                if (streq(actualRecord.queryName(actualIndex), fieldPath))
                {
                    actualColumns[fieldIndex] = actualIndex;
                    break;
                }
            }
            if (actualColumns[fieldIndex] == actualRecord.getNumFields())
                throw duckdb::BinderException("hpcc_scan could not map KEY field %s to the actual key layout", fieldPath);
        }

        for (auto& projectedColumn : state->projectedColumns)
            projectedColumn = actualColumns.at(projectedColumn);

        if (bindData.expectedRecord)
        {
            try
            {
                RowFilter actualFilters;
                Owned<const IKeyTranslator> translator = createKeyTranslator(actualRecord, bindData.expectedRecord->record);
                if (translator && translator->needsTranslate())
                    translator->translate(actualFilters, state->filters);
                else
                    actualFilters.appendFilters(state->filters);
                state->filters.kill();
                for (unsigned filterIndex = 0; filterIndex < actualFilters.numFilterFields(); ++filterIndex)
                    state->filters.append(OLINK(actualFilters.queryFilter(filterIndex)));
            }
            catch (IException* exception)
            {
                StringBuffer message;
                exception->errorMessage(message);
                exception->Release();
                throw duckdb::BinderException("hpcc_scan could not translate KEY filters: %s", message.str());
            }
        }
        else
        {
            for (unsigned filterIndex = 0; filterIndex < state->filters.ordinality(); ++filterIndex)
            {
                const unsigned publishedIndex = state->filters.item(filterIndex).queryFieldIndex();
                if (publishedIndex >= actualColumns.size())
                    throw duckdb::BinderException("hpcc_scan KEY filter references an unknown field");
                const unsigned actualIndex = actualColumns[publishedIndex];
                if (publishedIndex != actualIndex)
                {
                    Owned<IFieldFilter> remapped = state->filters.item(filterIndex).remap(actualIndex);
                    state->filters.replace(*remapped.getClear(), filterIndex);
                }
            }
        }
        mergeHpccScanFieldFilters(state->filters);

        // Each worker reads one data part, mirroring Thor's per-part local key managers.
        for (unsigned partIndex = 0; partIndex < bindData.offlinePartCopies.size(); ++partIndex)
        {
            if (bindData.offlineTopLevelKeyParts[partIndex])
                continue;
            Owned<IKeyIndex> keyIndex = openOfflineKeyFile(bindData.offlinePartCopies[partIndex], bindData.offlinePartCrcs[partIndex]);
            if (!keyIndex->isTopLevelKey())
                state->keyParts.append(*keyIndex.getClear());
        }
        if (!state->keyParts.ordinality())
            throw duckdb::IOException("hpcc_scan found no data parts in KEY file %s", bindData.logicalName);
        const unsigned keyedFields = actualRecord.getNumKeyedFields();
        RowFilter& keyedFilters = state->keyedFilters;
        std::vector<bool> constrained(keyedFields, false);
        ForEachItemIn(filterIndex, state->filters)
        {
            const unsigned fieldIndex = state->filters.item(filterIndex).queryFieldIndex();
            if (fieldIndex < keyedFields)
                constrained[fieldIndex] = true;
        }
        std::vector<bool> keyedAssigned(keyedFields, false);
        ForEachItemIn(applyIndex, state->filters)
        {
            const IFieldFilter& filter = state->filters.item(applyIndex);
            const unsigned fieldIndex = filter.queryFieldIndex();
            bool contiguousKeyed = fieldIndex < keyedFields;
            for (unsigned preceding = 0; contiguousKeyed && preceding < fieldIndex; ++preceding)
                contiguousKeyed = constrained[preceding];
            // A KEY accepts one keyed filter per field; range filters were merged above, so only filters that
            // cannot be intersected (wildcards and substrings) are left over and applied as post filters.
            const std::string description = describeHpccScanFieldFilter(filter, actualRecord.queryName(fieldIndex));
            if (contiguousKeyed && !keyedAssigned[fieldIndex])
            {
                keyedAssigned[fieldIndex] = true;
                keyedFilters.addFilter(OLINK(filter));
                state->keyedFilterDescriptions.push_back(description);
            }
            else
            {
                state->postFilters.addFilter(OLINK(filter));
                state->postFilterDescriptions.push_back(description);
            }
        }
        StringBuffer keyedFieldIndexes;
        for (unsigned filterIndex = 0; filterIndex < keyedFilters.numFilterFields(); ++filterIndex)
        {
            if (filterIndex)
                keyedFieldIndexes.append(',');
            keyedFieldIndexes.append(keyedFilters.queryFilter(filterIndex).queryFieldIndex());
        }
        try
        {
            keyedFilters.sortByFieldOrder();
        }
        catch (IException* exception)
        {
            StringBuffer message;
            exception->errorMessage(message);
            exception->Release();
            throw duckdb::IOException("hpcc_scan could not initialize KEY filters [%s]: %s", keyedFieldIndexes.str(), message.str());
        }
        return duckdb::unique_ptr<duckdb::GlobalTableFunctionState>(state.release());
    }

    return duckdb::unique_ptr<duckdb::GlobalTableFunctionState>(state.release());
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> initializeHpccScanLocal([[maybe_unused]] duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput&, duckdb::GlobalTableFunctionState*)
{
    auto state = duckdb::make_uniq<CScanLocalState>();
#ifdef _DEBUG
    state->scanObjectTracker.track(context.client, ScanObjectKind::LocalState);
#endif
    return duckdb::unique_ptr<duckdb::LocalTableFunctionState>(state.release());
}

// Reports the filters chosen at scan initialization, which include runtime join filters that are unknown when the plan
// is described, and replaces the plan-time Keyed/Post Filters entries in the query profile.
duckdb::InsertionOrderPreservingMap<std::string> describeHpccScanRuntime(duckdb::TableFunctionDynamicToStringInput& input)
{
    duckdb::InsertionOrderPreservingMap<std::string> description;
    if (!input.global_state)
        return description;
    const auto& state = input.global_state->Cast<CScanGlobalState>();
    const CScanBindData* bindData = input.bind_data ? &input.bind_data->Cast<CScanBindData>() : nullptr;
    // An empty runtime list still has to replace a non-empty plan-time entry of the same name.
    auto describe = [&](const char* name, const std::vector<std::string>& runtime, bool hadPlanEntry)
    {
        if (!runtime.empty())
            description[name] = joinHpccScanDescriptions(runtime);
        else if (hadPlanEntry)
            description[name] = "none";
    };
    describe("Keyed Filters", state.keyedFilterDescriptions, bindData && !bindData->keyedFilterDescriptions.empty());
    describe("Post Filters", state.postFilterDescriptions, bindData && !bindData->postFilterDescriptions.empty());
    return description;
}

duckdb::OperatorPartitionData getHpccScanPartitionData(duckdb::ClientContext&, duckdb::TableFunctionGetPartitionInput& input)
{
    if (input.partition_info.RequiresPartitionColumns())
        throw duckdb::InternalException("hpcc_scan does not support partition columns");
    return duckdb::OperatorPartitionData(input.local_state->Cast<CScanLocalState>().partIndex);
}

}

void duckdb::hpcc::Extension::Load(duckdb::ExtensionLoader& loader)
{
    loader.SetDescription("HPCC logical-file scan extension");
    auto function = duckdb::TableFunction("hpcc_scan", { duckdb::LogicalType::VARCHAR }, executeHpccScan, bindHpccScan, initializeHpccScan, initializeHpccScanLocal);
    function.varargs = duckdb::LogicalType::VARCHAR;
    function.get_partition_data = getHpccScanPartitionData;
    function.dynamic_to_string = describeHpccScanRuntime;
    function.projection_pushdown = true;
    function.filter_pushdown = true;
    function.filter_prune = true;
    configureHpccScanOptimizer(loader, function);
    loader.RegisterFunction(function);
}

std::string duckdb::hpcc::Extension::Name()
{
    return "hpcc_scan";
}

std::string duckdb::hpcc::Extension::Version() const
{
    return "0.1.0";
}

} // namespace duckdb::hpcc