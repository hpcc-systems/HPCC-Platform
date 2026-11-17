#include "metadata.hpp"

#include "fileview.hpp"
#include "dasess.hpp"
#include "jlib.hpp"
#include "jptree.hpp"
#include "rtldynfield.hpp"
#include "seclib.hpp"
#include "thorcommon.hpp"
#include "thorread.hpp"

#include "duckdb-hpcc/typemapping.hpp"
#include "duckdb-hpcc/materialization.hpp"
#include "duckdb-hpcc/extension.hpp"

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#endif

using namespace duckdb::hpcc;

#include <algorithm>
#include <type_traits>

IOutputMetaData* createWsSql2OutputMetaDataFromEcl(const char* ecl, const char* kind, bool grouped)
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

static void appendSqlSchemaField(StringBuffer& schema, const FieldDescriptor& field, bool& first)
{
    if (!first)
        schema.append(", ");
    first = false;
    schema.append('"').append(field.fieldPath.c_str()).append("\" ").append(field.duckdbType.ToString().c_str());
}

static void appendSqlLiteral(StringBuffer& output, const std::string& value)
{
    output.append('\'');
    for (char character : value)
    {
        if (character == '\'')
            output.append("''");
        else
            output.append(character);
    }
    output.append('\'');
}

static std::optional<uint64_t> queryKnownRecordCount(const IPropertyTree& attributes)
{
    if (!attributes.hasProp("@recordCount"))
        return std::nullopt;
    const int64_t recordCount = attributes.getPropInt64("@recordCount", -1);
    if (recordCount < 0)
        return std::nullopt;
    return static_cast<uint64_t>(recordCount);
}

static void populateScanParts(IDistributedFile& file, ScanMetadata& metadata)
{
    metadata.parts.clear();
    metadata.parts.reserve(file.numParts());
    for (unsigned partIndex = 0; partIndex < file.numParts(); ++partIndex)
    {
        Owned<IDistributedFilePart> part = file.getPart(partIndex);
        ScanPart scanPart;
        scanPart.topLevelKeyPart = isPartTLK(part->queryAttributes());
        unsigned crc = 0;
        if (part->getCrc(crc))
            scanPart.crc = crc;
        scanPart.copies.reserve(part->numCopies());
        for (unsigned copyIndex = 0; copyIndex < part->numCopies(); ++copyIndex)
        {
            RemoteFilename filename;
            part->getFilename(filename, copyIndex);
            StringBuffer path;
            filename.getRemotePath(path);
            scanPart.copies.emplace_back(path.str());
        }
        metadata.parts.emplace_back(std::move(scanPart));
    }
}

static void populateScanOptions(IDistributedFile& file, const FileAccessOptions& accessOptions, ScanMetadata& metadata)
{
    const IPropertyTree& attributes = file.queryAttributes();
    CDateTime modified;
    const __int64 modifiedTimestamp = file.getModificationTime(modified) ? static_cast<__int64>(modified.getSimple()) : 0;
    const char* logicalName = file.queryLogicalName();
    metadata.options.logicalName = isEmptyString(logicalName) ? ScanOptions::defaultLogicalName : logicalName;
    metadata.options.kind = attributes.queryProp("@kind", ScanOptions::defaultKind);
    metadata.options.format = accessOptions.format.isEmpty() ? ScanOptions::defaultFormat : accessOptions.format.str();
    metadata.options.grouped = attributes.getPropBool("@grouped", false);
    metadata.options.formatCrc = accessOptions.formatCrc;
    // ScanTranslationMode mirrors RecordTranslationMode; the extension static_asserts that the values match.
    metadata.options.recordTranslationMode = static_cast<ScanTranslationMode>(accessOptions.recordTranslationMode);
    metadata.options.modified = modifiedTimestamp;
    metadata.options.formatOptions.reset(accessOptions.formatOptions ? createPTreeFromIPT(accessOptions.formatOptions) : nullptr);
    metadata.options.providerOptions.reset(accessOptions.providerOptions ? createPTreeFromIPT(accessOptions.providerOptions) : createPTree("providerOptions"));
    if (!metadata.options.providerOptions->hasProp("@compressed"))
        metadata.options.providerOptions->setPropBool("@compressed", false);
}

bool CMetadataResolver::isValidLogicalName(const char* logicalName)
{
    CDfsLogicalFileName parsedName;
    return parseLogicalName(logicalName, parsedName) && !isExternalOrForeignName(parsedName);
}

// External names address physical files directly; foreign names select a Dali server that supplies physical metadata.
bool CMetadataResolver::isExternalOrForeignName(const CDfsLogicalFileName& parsedName)
{
    const unsigned numMembers = parsedName.multiOrdinality();
    if (parsedName.isExternal() || (numMembers == 0 && parsedName.isForeign()))
        return true;
    for (unsigned index = 0; index < numMembers; ++index)
    {
        const CDfsLogicalFileName& member = parsedName.multiItem(index);
        if (member.isExternal() || member.isForeign())
            return true;
    }
    return false;
}

bool CMetadataResolver::parseLogicalName(const char* logicalName, CDfsLogicalFileName& parsedName)
{
    if (isEmptyString(logicalName) || logicalName[0] == ':' || logicalName[strlen(logicalName) - 1] == ':')
        return false;

    const char* scopeSeparator = strstr(logicalName, "::");
    if (!scopeSeparator || scopeSeparator == logicalName || !scopeSeparator[2] || scopeSeparator[2] == ':')
        return false;

    for (const char* current = logicalName; *current; ++current)
    {
        if (isspace(static_cast<unsigned char>(*current)))
            return false;
        if (*current == ':')
        {
            if (current[1] != ':')
                return false;
            ++current;
        }
    }
    return parsedName.setValidate(logicalName);
}

Owned<IDistributedFile> CMetadataResolver::resolveLogicalFile(const char* logicalName, IUserDescriptor* user)
{
    CDfsLogicalFileName parsedName;
    if (!parseLogicalName(logicalName, parsedName))
        throw makeStringException(-1, "A valid scoped logical file name is required");
    if (isExternalOrForeignName(parsedName))
        throw makeStringException(-1, "External and foreign file names are not supported");

    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(parsedName, user, AccessMode::readRandom, false, false, nullptr, defaultNonPrivilegedUser);
    if (!file)
        throw makeStringExceptionV(-1, "Unable to resolve logical file '%s'", logicalName);
    return file;
}

// Shared by the query-time metadata and LogicalFileMetadata descriptor paths so both describe the file identically.
static void prepareScanMetadata(IDistributedFile& file, const char* recordDefinitionOverride, ScanMetadata& metadata, Owned<IOutputMetaData>& expectedMetadata, Owned<IOutputMetaData>& actualMetadata)
{
    const IPropertyTree& attributes = file.queryAttributes();
    StringBuffer fallbackEcl;
    const char* recordDefinition = recordDefinitionOverride ? recordDefinitionOverride : attributes.queryProp("ECL", "");
    if (!recordDefinitionOverride && isEmptyString(recordDefinition))
    {
        const unsigned recordSize = attributes.getPropInt("@recordSize", 0);
        if (recordSize)
        {
            fallbackEcl.appendf("RECORD string%u contents; END;", recordSize);
            recordDefinition = fallbackEcl.str();
        }
    }
    expectedMetadata.setown(getDaliLayoutInfo(attributes));
    if (!expectedMetadata && !isEmptyString(recordDefinition))
        expectedMetadata.setown(createWsSql2OutputMetaDataFromEcl(recordDefinition, attributes.queryProp("@kind", "flat"), attributes.getPropBool("@grouped", false)));
    if (!expectedMetadata)
        throw makeStringExceptionV(-1, "Unsupported record layout for '%s': runtime metadata is unavailable", file.queryLogicalName());
    if (hasUnsupportedRecordLayout(*expectedMetadata))
        throw makeStringExceptionV(-1, "Unsupported record layout for '%s': a compiled record helper would be required", file.queryLogicalName());

    FileAccessOptions accessOptions;
    accessOptions.formatOptions->setPropBool("@grouped", attributes.getPropBool("@grouped", false));
    accessOptions.updateFromFile(&file);
    actualMetadata.setown(accessOptions.actualDiskMeta.getLink());
    if (!actualMetadata)
        actualMetadata.set(expectedMetadata);
    if (hasUnsupportedRecordLayout(*actualMetadata))
        throw makeStringExceptionV(-1, "Unsupported record layout for '%s': a compiled record helper would be required", file.queryLogicalName());
    if (isEmptyString(recordDefinition))
        throw makeStringExceptionV(-1, "Unable to create scan metadata without an ECL record definition for '%s'", file.queryLogicalName());
    const bool isCsv = strieq(accessOptions.format.str(), "csv");
    const bool isXml = strieq(accessOptions.format.str(), "xml");
    if ((isCsv || isXml) && !accessOptions.formatCrc)
    {
        MemoryBuffer serializedLayout;
        if (dumpTypeInfo(serializedLayout, actualMetadata->queryTypeInfo()))
            accessOptions.formatCrc = hashc(serializedLayout.bytes(), serializedLayout.length(), 0) | 1;
    }
    if (isXml && attributes.hasProp("@rowTag") && !accessOptions.formatOptions->hasProp("ActivityOptions/rowTag"))
        ensurePTree(accessOptions.formatOptions, "ActivityOptions")->setProp("rowTag", attributes.queryProp("@rowTag"));

    metadata.recordDefinition = recordDefinition;
    populateScanParts(file, metadata);
    populateScanOptions(file, accessOptions, metadata);
    metadata.recordCount = queryKnownRecordCount(attributes);
}

void CMetadataResolver::buildScanMetadata(IDistributedFile& file, ScanMetadata& metadata, const char* recordDefinitionOverride)
{
    Owned<IOutputMetaData> expectedMetadata;
    Owned<IOutputMetaData> actualMetadata;
    prepareScanMetadata(file, recordDefinitionOverride, metadata, expectedMetadata, actualMetadata);
}

void CMetadataResolver::buildScanDescriptor(IDistributedFile& file, CScanDescriptor& descriptor, const char* recordDefinitionOverride)
{
    const IPropertyTree& attributes = file.queryAttributes();
    ScanMetadata scanMetadata;
    Owned<IOutputMetaData> expectedMetadata;
    Owned<IOutputMetaData> actualMetadata;
    prepareScanMetadata(file, recordDefinitionOverride, scanMetadata, expectedMetadata, actualMetadata);

    TypeMapper mapper;
    std::vector<FieldDescriptor> fields;
    std::vector<Warning> warnings;
    MemoryBuffer serializedMetadata;
    if (attributes.getPropBin("ECLbin", serializedMetadata))
    {
        Owned<IDefRecordMeta> recordMeta = deserializeRecordMeta(serializedMetadata, true);
        if (recordMeta)
            mapper.mapRecord(recordMeta->queryRecord(), nullptr, fields, warnings);
    }
    const RtlRecord& expectedRecord = expectedMetadata->queryRecordAccessor(true);
    const bool hasNestedDescriptor = std::any_of(fields.begin(), fields.end(), [](const FieldDescriptor& field) { return !field.children.empty(); });
    bool hasNestedExpectedField = false;
    for (unsigned index = 0; index < expectedRecord.getNumFields(); ++index)
    {
        const RtlTypeInfo* type = expectedRecord.queryType(index);
        if (type && (type->getType() == type_record || type->getType() == type_row || type->getType() == type_set || type->getType() == type_table || type->getType() == type_groupedtable))
        {
            hasNestedExpectedField = true;
            break;
        }
    }
    if (fields.empty() || (hasNestedExpectedField && !hasNestedDescriptor))
        fields = mapRtlRecord(expectedRecord);
    const RtlRecord& actualRecord = actualMetadata->queryRecordAccessor(true);
    bool hasNestedPhysicalField = false;
    for (unsigned index = 0; index < actualRecord.getNumFields(); ++index)
    {
        const RtlTypeInfo* type = actualRecord.queryType(index);
        if (type && (type->getType() == type_record || type->getType() == type_row || type->getType() == type_set || type->getType() == type_table || type->getType() == type_groupedtable))
        {
            hasNestedPhysicalField = true;
            break;
        }
    }
    if (hasNestedPhysicalField)
        fields = mapRtlRecord(actualRecord);
    StringBuffer sqlSchema;
    sqlSchema.append("CREATE TABLE \\\"scan\\\" (");
    bool first = true;
    for (const auto& field : fields)
        appendSqlSchemaField(sqlSchema, field, first);
    sqlSchema.append(");");

    descriptor.logicalName = file.queryLogicalName();
    descriptor.recordCount = scanMetadata.recordCount;
    descriptor.sqlSchema = sqlSchema.str();
    descriptor.eclRecordDefinition = scanMetadata.recordDefinition;
    descriptor.parts = scanMetadata.serializePartsJson();
    descriptor.options = scanMetadata.serializeOptionsJson();
    StringBuffer sampleSql;
    sampleSql.append("SELECT *\nFROM hpcc_scan(\n    ");
    appendSqlLiteral(sampleSql, descriptor.eclRecordDefinition);
    sampleSql.append(",\n    ");
    appendSqlLiteral(sampleSql, descriptor.parts);
    sampleSql.append(",\n    ");
    appendSqlLiteral(sampleSql, descriptor.options);
    sampleSql.append("\n)");
    descriptor.sampleSql = sampleSql.str();
}

#ifdef _USE_CPPUNIT
class WsSql2MetadataTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WsSql2MetadataTests);
    CPPUNIT_TEST(testLogicalNameValidation);
    CPPUNIT_TEST(testForeignLookupRejection);
    CPPUNIT_TEST(testPartCrcSerialization);
    CPPUNIT_TEST(testScanMetadataSerialization);
    CPPUNIT_TEST(testScanMetadataVersioning);
    CPPUNIT_TEST(testScanMetadataDefaults);
    CPPUNIT_TEST(testScanMetadataValidation);
    CPPUNIT_TEST(testScanMetadataCopy);
    CPPUNIT_TEST(testScanDescriptorRoundTrip);
    CPPUNIT_TEST(testRecordSizeOnlyDescriptor);
    CPPUNIT_TEST(testKnownRecordCount);
    CPPUNIT_TEST(testRuntimeRecordMapping);
    CPPUNIT_TEST(testRuntimeDecimalTypes);
    CPPUNIT_TEST(testRuntimeSerialIfBlockSupport);
    CPPUNIT_TEST(testRuntimeNestedBlobMapping);
    CPPUNIT_TEST_SUITE_END();

public:
    void testLogicalNameValidation()
    {
        CPPUNIT_ASSERT(CMetadataResolver::isValidLogicalName("scope::file"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("file"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("scope::bad name"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("scope:::file"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("scope::::file"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("scope:file"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("file::127.0.0.1::etc::passwd"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("~file::127.0.0.1::etc::passwd"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("{scope::file,~file::127.0.0.1::etc::passwd}"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("foreign::127.0.0.1::scope::file"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("{scope::file,~foreign::127.0.0.1::scope::other}"));
        CPPUNIT_ASSERT(!CMetadataResolver::isValidLogicalName("{~foreign::127.0.0.1::scope::file,~scope::other}"));
        CPPUNIT_ASSERT(CMetadataResolver::isValidLogicalName("{scope::file,~scope::other}"));
    }

    void testForeignLookupRejection()
    {
        const char* names[] = {
            "foreign::127.0.0.1::scope::file",
            "{scope::file,~foreign::127.0.0.1::scope::other}"
        };
        for (const char* name : names)
        {
            try
            {
                CMetadataResolver::resolveLogicalFile(name, nullptr);
                CPPUNIT_FAIL("Foreign logical file name was not rejected before lookup");
            }
            catch (IException* error)
            {
                StringBuffer message;
                error->errorMessage(message);
                error->Release();
                CPPUNIT_ASSERT_EQUAL(std::string("External and foreign file names are not supported"), std::string(message.str()));
            }
        }
    }

    void testPartCrcSerialization()
    {
        ScanMetadata metadata;
        metadata.parts.push_back({ { "/data/without-crc" }, std::nullopt, false });
        metadata.parts.push_back({ { "/data/with-crc" }, UINT_MAX, false });

        const std::string serialized = metadata.serializePartsJson();
        CPPUNIT_ASSERT(serialized.find("\"@crc\"") != std::string::npos);
        CPPUNIT_ASSERT_EQUAL(std::string::npos, serialized.find("\"@crc\"", serialized.find("\"@crc\"") + 1));
        CPPUNIT_ASSERT(serialized.find("4294967295") != std::string::npos);
    }

    void testScanMetadataSerialization()
    {
        ScanMetadata original;
        original.recordDefinition = "RECORD unsigned4 id; END;";
        original.parts.push_back({ { "/data/part1" }, UINT_MAX, true });
        original.options.logicalName = "scope::file";
        original.options.kind = "key";
        original.options.format = "flat";
        original.options.grouped = true;
        original.options.formatCrc = 42;
        original.options.recordTranslationMode = ScanTranslationMode::AlwaysDisk;
        original.options.modified = 123456;
        original.options.formatOptions.reset(createPTree("formatOptions"));
        original.options.formatOptions->setPropInt("@header", 1);
        original.options.providerOptions.reset(createPTree("providerOptions"));
        original.options.providerOptions->setPropBool("@compressed", true);

        ScanMetadata decoded;
        decoded.deserializeLongForm(original.recordDefinition, original.serializePartsJson(), original.serializeOptionsJson());

        CPPUNIT_ASSERT_EQUAL(original.recordDefinition, decoded.recordDefinition);
        CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, decoded.parts.size());
        CPPUNIT_ASSERT_EQUAL(std::string("/data/part1"), decoded.parts[0].copies[0]);
        CPPUNIT_ASSERT(decoded.parts[0].crc && *decoded.parts[0].crc == UINT_MAX);
        CPPUNIT_ASSERT(decoded.parts[0].topLevelKeyPart);
        CPPUNIT_ASSERT_EQUAL(original.options.logicalName, decoded.options.logicalName);
        CPPUNIT_ASSERT_EQUAL(original.options.kind, decoded.options.kind);
        CPPUNIT_ASSERT_EQUAL(original.options.format, decoded.options.format);
        CPPUNIT_ASSERT(decoded.options.grouped);
        CPPUNIT_ASSERT_EQUAL(original.options.formatCrc, decoded.options.formatCrc);
        CPPUNIT_ASSERT(original.options.recordTranslationMode == decoded.options.recordTranslationMode);
        CPPUNIT_ASSERT_EQUAL(original.options.modified, decoded.options.modified);
        CPPUNIT_ASSERT_EQUAL(1, decoded.options.formatOptions->getPropInt("@header", 0));
        CPPUNIT_ASSERT(decoded.options.providerOptions->getPropBool("@compressed", false));
    }

    static bool deserializeFails(const char* partsJson, const char* optionsJson, const char* expectedMessage)
    {
        ScanMetadata metadata;
        try
        {
            metadata.deserializeLongForm("RECORD unsigned4 id; END;", partsJson, optionsJson);
        }
        catch (const std::exception& exception)
        {
            return strstr(exception.what(), expectedMessage) != nullptr;
        }
        return false;
    }

    static bool deserializeFails(const char* optionsJson, const char* expectedMessage)
    {
        return deserializeFails("{\"part\":{\"copy\":\"/data/part1\"}}", optionsJson, expectedMessage);
    }

    void testScanMetadataVersioning()
    {
        CPPUNIT_ASSERT_EQUAL(1U, ScanMetadata::optionsVersion);
        ScanMetadata current;
        current.recordDefinition = "RECORD unsigned4 id; END;";
        current.parts.push_back({ { "/data/part1" }, std::nullopt, false });
        Owned<IPropertyTree> serializedOptions = createPTreeFromJSONString(current.serializeOptionsJson().c_str());
        CPPUNIT_ASSERT_EQUAL(static_cast<int>(ScanMetadata::optionsVersion), serializedOptions->getPropInt("@version", 0));

        // Hand-written options may omit @version.
        ScanMetadata versionless;
        versionless.deserializeLongForm(current.recordDefinition, current.serializePartsJson(), "{\"@logicalName\":\"scope::versionless\"}");
        CPPUNIT_ASSERT_EQUAL(std::string("scope::versionless"), versionless.options.logicalName);

        VStringBuffer newerOptions("{\"@version\":%u}", ScanMetadata::optionsVersion + 1);
        CPPUNIT_ASSERT(deserializeFails(newerOptions.str(), "unsupported options version"));
        CPPUNIT_ASSERT(deserializeFails("{\"@version\":0}", "unsupported options version"));
    }

    void testScanMetadataDefaults()
    {
        ScanMetadata typed;
        CPPUNIT_ASSERT_EQUAL(std::string(ScanOptions::defaultLogicalName), typed.options.logicalName);
        CPPUNIT_ASSERT_EQUAL(std::string(ScanOptions::defaultKind), typed.options.kind);
        CPPUNIT_ASSERT_EQUAL(std::string(ScanOptions::defaultFormat), typed.options.format);

        // Missing and empty JSON values both resolve to the same defaults as a default-constructed ScanOptions.
        const char* const optionsVariants[] = { "{}", "{\"@logicalName\":\"\",\"@kind\":\"\",\"@format\":\"\"}" };
        for (const char* optionsJson : optionsVariants)
        {
            ScanMetadata decoded;
            decoded.deserializeLongForm("RECORD unsigned4 id; END;", "{\"part\":[{\"copy\":\"/data/part1\"},{\"@tlk\":true}]}", optionsJson);
            CPPUNIT_ASSERT_EQUAL(typed.options.logicalName, decoded.options.logicalName);
            CPPUNIT_ASSERT_EQUAL(typed.options.kind, decoded.options.kind);
            CPPUNIT_ASSERT_EQUAL(typed.options.format, decoded.options.format);
            CPPUNIT_ASSERT(!decoded.options.grouped);
            CPPUNIT_ASSERT_EQUAL(0U, decoded.options.formatCrc);
            CPPUNIT_ASSERT(decoded.options.recordTranslationMode == ScanTranslationMode::Unspecified);
            CPPUNIT_ASSERT(!decoded.recordCount);
            // The part without copies cannot be read, so it is dropped.
            CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, decoded.parts.size());
            CPPUNIT_ASSERT_EQUAL(std::string("/data/part1"), decoded.parts[0].copies[0]);
            CPPUNIT_ASSERT(!decoded.parts[0].crc);
        }
    }

    void testScanMetadataValidation()
    {
        CPPUNIT_ASSERT(deserializeFails("{\"formatOptions\":\"text\"}", "formatOptions to be a JSON object"));
        // jptree flattens JSON arrays into repeated elements, so only multi-element arrays are detectable.
        CPPUNIT_ASSERT(deserializeFails("{\"formatOptions\":[{\"@header\":1},{\"@header\":2}]}", "formatOptions to be a JSON object"));
        CPPUNIT_ASSERT(deserializeFails("{\"providerOptions\":5}", "providerOptions to be a JSON object"));
        CPPUNIT_ASSERT(deserializeFails("{\"@recordTranslationMode\":1}", "invalid recordTranslationMode"));
        CPPUNIT_ASSERT(deserializeFails("{\"@recordTranslationMode\":7}", "invalid recordTranslationMode"));
        CPPUNIT_ASSERT(deserializeFails("{\"part\":{\"@crc\":4294967296,\"copy\":\"/data/part1\"}}", "{}", "outside the unsigned 32-bit range"));
        CPPUNIT_ASSERT(deserializeFails("{\"part\":{\"@crc\":-1,\"copy\":\"/data/part1\"}}", "{}", "outside the unsigned 32-bit range"));

        // A rejected payload leaves previously decoded metadata untouched.
        ScanMetadata metadata;
        metadata.deserializeLongForm("RECORD unsigned4 id; END;", "{\"part\":{\"copy\":\"/data/part1\"}}", "{\"@logicalName\":\"scope::kept\",\"formatOptions\":{\"@header\":1}}");
        CPPUNIT_ASSERT_THROW(metadata.deserializeLongForm("RECORD unsigned4 other; END;", "{\"part\":{\"copy\":\"/data/other\"}}", "{\"formatOptions\":\"text\"}"), std::exception);
        CPPUNIT_ASSERT_EQUAL(std::string("scope::kept"), metadata.options.logicalName);
        CPPUNIT_ASSERT_EQUAL(std::string("/data/part1"), metadata.parts[0].copies[0]);
        CPPUNIT_ASSERT_EQUAL(1, metadata.options.formatOptions->getPropInt("@header", 0));

        ScanMetadata accepted;
        accepted.deserializeLongForm("RECORD unsigned4 id; END;", "{\"part\":{\"@crc\":4294967295,\"copy\":\"/data/part1\"}}", "{\"@recordTranslationMode\":5,\"formatOptions\":{},\"providerOptions\":{\"@compressed\":true}}");
        CPPUNIT_ASSERT(accepted.options.recordTranslationMode == ScanTranslationMode::PayloadRemoveOnly);
        CPPUNIT_ASSERT(accepted.options.formatOptions);
        CPPUNIT_ASSERT(accepted.options.providerOptions->getPropBool("@compressed", false));
        CPPUNIT_ASSERT_EQUAL(UINT_MAX, *accepted.parts[0].crc);
    }

    void testScanMetadataCopy()
    {
        static_assert(!std::is_copy_constructible_v<ScanMetadata>);
        static_assert(!std::is_copy_assignable_v<ScanMetadata>);
        static_assert(std::is_move_constructible_v<ScanMetadata>);

        ScanMetadata original;
        original.recordDefinition = "RECORD unsigned4 id; END;";
        original.parts.push_back({ { "/data/part1" }, 7U, true });
        original.recordCount = 12;
        original.options.formatOptions.reset(createPTree("formatOptions"));
        original.options.formatOptions->setPropInt("@header", 1);

        ScanMetadata copy;
        original.copyTo(copy);
        original.options.formatOptions->setPropInt("@header", 2);
        CPPUNIT_ASSERT(copy.options.formatOptions.get() != original.options.formatOptions.get());
        CPPUNIT_ASSERT_EQUAL(1, copy.options.formatOptions->getPropInt("@header", 0));
        CPPUNIT_ASSERT(!copy.options.providerOptions);
        CPPUNIT_ASSERT_EQUAL(original.recordDefinition, copy.recordDefinition);
        CPPUNIT_ASSERT_EQUAL(7U, *copy.parts[0].crc);
        CPPUNIT_ASSERT_EQUAL(uint64_t{ 12 }, *copy.recordCount);
    }

    // LogicalFileMetadata publishes the descriptor's JSON for use with the long-form hpcc_scan overload, so decoding it
    // must reproduce the typed metadata used by hpcc_scan(logicalName).
    void testScanDescriptorRoundTrip()
    {
        // An inline group lets the descriptor resolve part locations without Dali's named group store.
        Owned<IPropertyTree> fileTree = createPTreeFromXMLString(
            "<File directory='/var/lib/HPCCSystems/hpcc-data/wssql2' partmask='descriptor._$P$_of_$N$' numparts='2'>"
            "<Cluster><Group>127.0.0.1-2</Group></Cluster>"
            "<Attr recordCount='42' recordSize='10'><ECL>RECORD unsigned4 id; string10 name; END;</ECL></Attr>"
            "</File>");
        Owned<IFileDescriptor> fileDesc = deserializeFileDescriptorTree(fileTree);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fileDesc, "test::wssql2::descriptor");

        CScanDescriptor descriptor;
        CMetadataResolver::buildScanDescriptor(*file, descriptor, nullptr);
        ScanMetadata typed;
        CMetadataResolver::buildScanMetadata(*file, typed, nullptr);
        ScanMetadata decoded;
        decoded.deserializeLongForm(descriptor.eclRecordDefinition, descriptor.parts, descriptor.options);

        CPPUNIT_ASSERT_EQUAL(std::string("RECORD unsigned4 id; string10 name; END;"), descriptor.eclRecordDefinition);
        CPPUNIT_ASSERT_EQUAL(typed.recordDefinition, decoded.recordDefinition);
        CPPUNIT_ASSERT(typed.recordCount && descriptor.recordCount);
        CPPUNIT_ASSERT_EQUAL(*typed.recordCount, *descriptor.recordCount);
        CPPUNIT_ASSERT_EQUAL(size_t{ 2 }, typed.parts.size());
        CPPUNIT_ASSERT_EQUAL(typed.parts.size(), decoded.parts.size());
        for (size_t part = 0; part < typed.parts.size(); ++part)
        {
            CPPUNIT_ASSERT(typed.parts[part].copies == decoded.parts[part].copies);
            CPPUNIT_ASSERT(typed.parts[part].crc == decoded.parts[part].crc);
            CPPUNIT_ASSERT_EQUAL(typed.parts[part].topLevelKeyPart, decoded.parts[part].topLevelKeyPart);
            CPPUNIT_ASSERT(strstr(decoded.parts[part].copies[0].c_str(), "descriptor._") != nullptr);
        }
        CPPUNIT_ASSERT_EQUAL(std::string("test::wssql2::descriptor"), decoded.options.logicalName);
        CPPUNIT_ASSERT_EQUAL(typed.options.logicalName, decoded.options.logicalName);
        CPPUNIT_ASSERT_EQUAL(typed.options.kind, decoded.options.kind);
        CPPUNIT_ASSERT_EQUAL(typed.options.format, decoded.options.format);
        CPPUNIT_ASSERT_EQUAL(typed.options.grouped, decoded.options.grouped);
        CPPUNIT_ASSERT_EQUAL(typed.options.formatCrc, decoded.options.formatCrc);
        CPPUNIT_ASSERT(typed.options.recordTranslationMode == decoded.options.recordTranslationMode);
        CPPUNIT_ASSERT_EQUAL(typed.options.modified, decoded.options.modified);
        // Serializing the typed metadata must yield exactly the JSON that LogicalFileMetadata publishes.
        CPPUNIT_ASSERT_EQUAL(descriptor.parts, typed.serializePartsJson());
        CPPUNIT_ASSERT_EQUAL(descriptor.options, typed.serializeOptionsJson());
        CPPUNIT_ASSERT_EQUAL(descriptor.options, decoded.serializeOptionsJson());
        CPPUNIT_ASSERT(descriptor.sampleSql.find("hpcc_scan(") != std::string::npos);
    }

    void testRecordSizeOnlyDescriptor()
    {
        Owned<IPropertyTree> fileTree = createPTreeFromXMLString(
            "<File directory='/var/lib/HPCCSystems/hpcc-data/wssql2' partmask='recordsize._$P$_of_$N$' numparts='1'>"
            "<Cluster><Group>127.0.0.1</Group></Cluster>"
            "<Attr recordSize='10'/>"
            "</File>");
        Owned<IFileDescriptor> fileDesc = deserializeFileDescriptorTree(fileTree);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fileDesc, "test::wssql2::recordsize");

        CScanDescriptor descriptor;
        CMetadataResolver::buildScanDescriptor(*file, descriptor, nullptr);
        ScanMetadata scanMetadata;
        CMetadataResolver::buildScanMetadata(*file, scanMetadata, nullptr);
        ScanMetadata decoded;
        decoded.deserializeLongForm(descriptor.eclRecordDefinition, descriptor.parts, descriptor.options);
        CPPUNIT_ASSERT_EQUAL(std::string("RECORD string10 contents; END;"), descriptor.eclRecordDefinition);
        CPPUNIT_ASSERT_EQUAL(descriptor.eclRecordDefinition, scanMetadata.recordDefinition);
        CPPUNIT_ASSERT_EQUAL(scanMetadata.recordDefinition, decoded.recordDefinition);
        CPPUNIT_ASSERT_EQUAL(scanMetadata.serializePartsJson(), descriptor.parts);
        CPPUNIT_ASSERT_EQUAL(scanMetadata.serializeOptionsJson(), descriptor.options);
        CPPUNIT_ASSERT(descriptor.sqlSchema.find("\"contents\" VARCHAR") != std::string::npos);
        CPPUNIT_ASSERT(descriptor.sampleSql.find("RECORD string10 contents; END;") != std::string::npos);
    }

    void testKnownRecordCount()
    {
        Owned<IPropertyTree> attributes = createPTree("Attr");
        CPPUNIT_ASSERT(!queryKnownRecordCount(*attributes));
        attributes->setPropInt64("@recordCount", 123456789);
        CPPUNIT_ASSERT_EQUAL(uint64_t(123456789), queryKnownRecordCount(*attributes).value());
        attributes->setPropInt64("@recordCount", -2);
        CPPUNIT_ASSERT(!queryKnownRecordCount(*attributes));
    }

    void testRuntimeRecordMapping()
    {
        Owned<IOutputMetaData> metadata = createWsSql2OutputMetaDataFromEcl(
            "child := RECORD unsigned4 childId; END; RECORD unsigned8 id; DATASET(child) children; END;",
            "flat", false);
        CPPUNIT_ASSERT(metadata);

        const std::vector<FieldDescriptor> fields = mapRtlRecord(metadata->queryRecordAccessor(true));
        CPPUNIT_ASSERT_EQUAL(size_t{ 2 }, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("id"), fields[0].fieldPath);
        CPPUNIT_ASSERT(fields[0].duckdbType == duckdb::LogicalType::UBIGINT);
        CPPUNIT_ASSERT_EQUAL(std::string("children"), fields[1].fieldPath);
        CPPUNIT_ASSERT(fields[1].duckdbType == duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
        CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, fields[1].children.size());
        CPPUNIT_ASSERT_EQUAL(std::string("children.childid"), fields[1].children[0].fieldPath);
    }

    void testRuntimeSerialIfBlockSupport()
    {
        Owned<IOutputMetaData> metadata = createWsSql2OutputMetaDataFromEcl(
            "RECORD unsigned1 flag; IFBLOCK(SELF.flag = 1) string10 value; END; END;",
            "flat", false);
        CPPUNIT_ASSERT(metadata);
        CPPUNIT_ASSERT(metadata->queryRecordAccessor(true).getNumIfBlocks());
        CPPUNIT_ASSERT(!hasUnsupportedRecordLayout(*metadata));
    }

    void testRuntimeDecimalTypes()
    {
        Owned<IOutputMetaData> metadata = createWsSql2OutputMetaDataFromEcl(
            "RECORD decimal12_4 amount; udecimal18_2 total; decimal20_2 large; udecimal38_6 largest; END;",
            "flat", false);
        CPPUNIT_ASSERT(metadata);
        const std::vector<FieldDescriptor> fields = mapRtlRecord(metadata->queryRecordAccessor(true));
        CPPUNIT_ASSERT_EQUAL(size_t{ 4 }, fields.size());
        CPPUNIT_ASSERT(fields[0].duckdbType == duckdb::LogicalType::DECIMAL(12, 4));
        CPPUNIT_ASSERT(fields[1].duckdbType == duckdb::LogicalType::DECIMAL(18, 2));
        CPPUNIT_ASSERT(fields[2].duckdbType == duckdb::LogicalType::DECIMAL(20, 2));
        CPPUNIT_ASSERT(fields[3].duckdbType == duckdb::LogicalType::DECIMAL(38, 6));

        duckdb::vector<duckdb::LogicalType> types;
        duckdb::vector<std::string> names;
        appendOfflineScanColumns(*metadata, fields, types, names);
        CPPUNIT_ASSERT_EQUAL(fields.size(), types.size());
        StringBuffer schema;
        bool first = true;
        for (size_t index = 0; index < fields.size(); ++index)
        {
            CPPUNIT_ASSERT(types[index] == fields[index].duckdbType);
            CPPUNIT_ASSERT_EQUAL(fields[index].fieldPath, names[index]);
            appendSqlSchemaField(schema, fields[index], first);
        }
        CPPUNIT_ASSERT_EQUAL(std::string("\"amount\" DECIMAL(12,4), \"total\" DECIMAL(18,2), \"large\" DECIMAL(20,2), \"largest\" DECIMAL(38,6)"), std::string(schema.str()));
    }

    void testRuntimeNestedBlobMapping()
    {
        Owned<IOutputMetaData> metadata = createWsSql2OutputMetaDataFromEcl(
            "nested := RECORD udecimal8 booklimit := 0; END; RECORD nested limit{blob}; DATASET(nested) children{blob}; DATA4 payload; STRING100 blobfield{blob}; END;",
            "key", false);
        CPPUNIT_ASSERT(metadata);

        const std::vector<FieldDescriptor> fields = mapRtlRecord(metadata->queryRecordAccessor(true));
        CPPUNIT_ASSERT_EQUAL(size_t{ 4 }, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("limit"), fields[0].fieldPath);
        CPPUNIT_ASSERT(fields[0].duckdbType == duckdb::LogicalType::STRUCT({ { "booklimit", duckdb::LogicalType::DECIMAL(8, 0) } }));
        CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, fields[0].children.size());
        CPPUNIT_ASSERT_EQUAL(std::string("limit.booklimit"), fields[0].children[0].fieldPath);
        CPPUNIT_ASSERT(fields[0].children[0].duckdbType == duckdb::LogicalType::DECIMAL(8, 0));
        CPPUNIT_ASSERT_EQUAL(std::string("children"), fields[1].fieldPath);
        CPPUNIT_ASSERT(fields[1].duckdbType == duckdb::LogicalType::LIST(duckdb::LogicalType::DECIMAL(8, 0)));
        CPPUNIT_ASSERT_EQUAL(std::string("children.booklimit"), fields[1].children[0].fieldPath);
        CPPUNIT_ASSERT(fields[2].duckdbType == duckdb::LogicalType::BLOB);
        CPPUNIT_ASSERT(fields[3].duckdbType == duckdb::LogicalType::VARCHAR);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(WsSql2MetadataTests);
#endif // _USE_CPPUNIT