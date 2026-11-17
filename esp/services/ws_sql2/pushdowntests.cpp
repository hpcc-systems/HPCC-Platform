#include "engine.hpp"
#include "duckdb-hpcc/extension.hpp"

#include "unittests.hpp"
#include "jfile.hpp"
#include "jutil.hpp"
#include "nbcd.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

// Differential tests for hpcc_scan filter, Top-N and join pushdown. Every query is executed twice: once
// directly against hpcc_scan, where DuckDB may push table filters (including optional, dynamic Top-N and
// join filters) into the scan, and once with the scan wrapped below a LIMIT, which blocks all pushdown so
// DuckDB evaluates every predicate itself. Both runs must succeed and return identical results.

namespace
{

constexpr unsigned numParts = 3;
constexpr unsigned numRows = 3000;
// Straddles INT64_MAX, so unsigned8 values must be read, filtered and returned as UBIGINT.
constexpr uint64_t bigBase = 9223372036854775807ULL - 2500;

struct TestRow
{
    uint32_t id;
    int32_t grp;
    uint64_t big;
    char tag[4];
    uint8_t small;
};

TestRow makeRow(unsigned index)
{
    TestRow row;
    row.id = index;
    row.grp = static_cast<int32_t>((index * 37) % 101) - 50;
    row.big = bigBase + (index * 7919ULL) % 5003;
    std::string tag = "t" + std::to_string(index % 10);
    tag.resize(sizeof(row.tag), ' ');
    memcpy(row.tag, tag.data(), sizeof(row.tag));
    row.small = static_cast<uint8_t>(index % 251);
    return row;
}

void writeLittleEndian(FILE* file, uint64_t value, unsigned size)
{
    for (unsigned byte = 0; byte < size; ++byte)
        CPPUNIT_ASSERT(fputc(static_cast<int>((value >> (8 * byte)) & 0xff), file) != EOF);
}

struct CFileCloser
{
    void operator()(FILE* file) const
    {
        if (file)
            fclose(file);
    }
};

using TestFile = std::unique_ptr<FILE, CFileCloser>;

void writeString(FILE* file, const std::string& value);

TestFile openTestFile(const std::string& path)
{
    FILE* file = fopen(path.c_str(), "wb");
    CPPUNIT_ASSERT_MESSAGE("Could not create " + path, file != nullptr);
    return TestFile(file);
}

void closeTestFile(TestFile& file)
{
    CPPUNIT_ASSERT_EQUAL(0, fclose(file.release()));
}

std::string writeParts(const std::string& directory)
{
    std::string partsJson = "{\"part\":[";
    for (unsigned part = 0; part < numParts; ++part)
    {
        const std::string path = directory + "/pushdown._" + std::to_string(part + 1) + "_of_" + std::to_string(numParts);
        auto file = openTestFile(path);
        for (unsigned index = part; index < numRows; index += numParts)
        {
            const TestRow row = makeRow(index);
            writeLittleEndian(file.get(), row.id, 4);
            writeLittleEndian(file.get(), static_cast<uint32_t>(row.grp), 4);
            writeLittleEndian(file.get(), row.big, 8);
            CPPUNIT_ASSERT_EQUAL(sizeof(row.tag), fwrite(row.tag, 1, sizeof(row.tag), file.get()));
            writeLittleEndian(file.get(), row.small, 1);
            writeString(file.get(), "n" + std::string(index % 7, 'q'));
        }
        closeTestFile(file);
        if (part)
            partsJson += ",";
        partsJson += "{\"@tlk\":\"0\",\"copy\":[\"" + path + "\"]}";
    }
    return partsJson + "]}";
}

class CPushdownTester
{
public:
    CPushdownTester(CQueryEngine& _engine, const std::string& _scan) : engine(_engine), scan(_scan)
    {
    }

    // {T} in the SQL template is replaced with the scan under test.
    void check(const std::string& sqlTemplate, bool expectRows = true)
    {
        const std::string pushedSql = expand(sqlTemplate, scan);
        const std::string oracleSql = expand(sqlTemplate, "(SELECT * FROM " + scan + " LIMIT 1000000000)");
        std::string pushed;
        std::string oracle;
        std::string error;
        const bool pushedQuerySucceeded = engine.executeQuery(pushedSql.c_str(), 0, 0, pushed, error);
        CPPUNIT_ASSERT_MESSAGE("Pushed query failed: " + pushedSql + "\n" + error, pushedQuerySucceeded);
        const bool oracleQuerySucceeded = engine.executeQuery(oracleSql.c_str(), 0, 0, oracle, error);
        CPPUNIT_ASSERT_MESSAGE("Oracle query failed: " + oracleSql + "\n" + error, oracleQuerySucceeded);
        if (pushed != oracle)
        {
            std::string planText;
            std::string planJson;
            StringArray projected;
            StringArray residual;
            std::vector<CQueryResultColumnMetadata> resultMetadata;
            const bool explainSucceeded = engine.explainQuery(pushedSql.c_str(), planText, planJson, projected, residual, resultMetadata, error);
            CPPUNIT_ASSERT_MESSAGE("Results differ for: " + sqlTemplate + "\nPushed: " + pushed.substr(0, 400) + "\nOracle: " + oracle.substr(0, 400) + "\nPushed plan: " + planText + (explainSucceeded ? "" : "\nExplain failed: " + error), pushed == oracle);
        }
        CPPUNIT_ASSERT_MESSAGE("Expected a non-empty result for: " + sqlTemplate, !expectRows || pushed.find("\"returnedRowCount\":0,") == std::string::npos);
    }

    void checkPushed(const std::string& sqlTemplate, const std::string& field)
    {
        check(sqlTemplate);
        std::string planText;
        std::string planJson;
        StringArray projected;
        StringArray residual;
        std::vector<CQueryResultColumnMetadata> resultMetadata;
        std::string error;
        const std::string sql = expand(sqlTemplate, scan);
        const bool explainSucceeded = engine.explainQuery(sql.c_str(), planText, planJson, projected, residual, resultMetadata, error);
        CPPUNIT_ASSERT_MESSAGE("Could not explain pushed query: " + sql + "\n" + error, explainSucceeded);
        CPPUNIT_ASSERT_MESSAGE("Expected a scan filter for " + field + " in plan:\n" + planText,
            planText.find("Filters:") != std::string::npos && planText.find(field) != std::string::npos);
    }

    void checkResidual(const std::string& sqlTemplate, const std::string& field, bool expectRows = true)
    {
        check(sqlTemplate, expectRows);
        std::string planText;
        std::string planJson;
        StringArray projected;
        StringArray residual;
        std::vector<CQueryResultColumnMetadata> resultMetadata;
        std::string error;
        const std::string sql = expand(sqlTemplate, scan);
        const bool explainSucceeded = engine.explainQuery(sql.c_str(), planText, planJson, projected, residual, resultMetadata, error);
        CPPUNIT_ASSERT_MESSAGE("Could not explain residual query: " + sql + "\n" + error, explainSucceeded);
        CPPUNIT_ASSERT_MESSAGE("Expected a residual filter for " + field + " in plan:\n" + planText,
            planText.find("FILTER") != std::string::npos && planText.find(field) != std::string::npos);
    }

private:
    static std::string expand(const std::string& sqlTemplate, const std::string& table)
    {
        std::string sql = sqlTemplate;
        for (size_t position = sql.find("{T}"); position != std::string::npos; position = sql.find("{T}", position))
        {
            sql.replace(position, 3, table);
            position += table.size();
        }
        return sql;
    }

    CQueryEngine& engine;
    std::string scan;
};

void runPredicateTests(CPushdownTester& tester)
{
    struct ColumnCase
    {
        const char* name;
        std::string low;
        std::string mid;
        std::string high;
        std::string sparseIn;
        std::string denseIn;
    };
    const std::string bigLow = std::to_string(bigBase + 100);
    const std::string bigMid = std::to_string(makeRow(1).big);
    const std::string bigHigh = std::to_string(bigBase + 4900);
    const ColumnCase columns[] = {
        { "id", "100", "1500", "2900", "3, 17, 400, 2999", "10, 11, 12, 13" },
        { "grp", "-40", "0", "40", "-50, -7, 3, 50", "-2, -1, 0, 1, 2" },
        { "big", bigLow, bigMid, bigHigh,
         std::to_string(makeRow(0).big) + ", " + std::to_string(makeRow(5).big) + ", " + std::to_string(makeRow(9).big),
         bigMid + ", " + std::to_string(makeRow(1).big + 1) + ", " + std::to_string(makeRow(1).big + 2) },
        { "small", "10", "100", "240", "0, 7, 25, 250", "100, 101, 102" },
    };
    for (const ColumnCase& column : columns)
    {
        const std::string x = column.name;
        const std::string select = "SELECT id, grp, big, tag, small FROM {T} WHERE ";
        const std::string order = " ORDER BY id";
        const std::vector<std::string> predicates = {
            x + " = " + column.mid,
            x + " <> " + column.mid,
            x + " < " + column.mid,
            x + " <= " + column.mid,
            x + " > " + column.mid,
            x + " >= " + column.mid,
            x + " BETWEEN " + column.low + " AND " + column.high,
            x + " > " + column.low + " AND " + x + " > " + column.mid + " AND " + x + " < " + column.high,
            x + " IN (" + column.denseIn + ")",
            x + " IN (" + column.sparseIn + ")",
            x + " NOT IN (" + column.sparseIn + ")",
            // Non-dense IN lists and same-column ORs are pushed as optional filters; ANDed with a mandatory
            // comparison on the same column they arrive as one CONJUNCTION_AND containing an OPTIONAL_FILTER.
            x + " IN (" + column.sparseIn + ") AND " + x + " > " + column.low,
            x + " IN (" + column.sparseIn + ") AND " + x + " BETWEEN " + column.low + " AND " + column.high,
            "(" + x + " < " + column.low + " OR " + x + " > " + column.high + ")",
            "(" + x + " < " + column.low + " OR " + x + " > " + column.high + ") AND " + x + " <> " + column.mid,
            x + " IS NOT NULL",
            x + " IS NULL OR " + x + " < " + column.mid,
            x + " IS NOT NULL AND " + x + " >= " + column.mid,
        };
        for (const std::string& predicate : predicates)
            tester.check(select + predicate + order);
        tester.check(select + x + " IS NULL" + order, false);
        tester.check(select + x + " > " + column.high + " AND " + x + " < " + column.low + order, false);
        tester.check(select + x + " = " + column.low + " AND " + x + " = " + column.high + order, false);
    }
    // HPCC integers map to BIGINT (UBIGINT for unsigned8), so constants can lie outside the field's own range; they
    // must not wrap when converted to an HPCC value set (e.g. -1 compared with an unsigned field).
    struct RangeLimitCase
    {
        const char* name;
        std::vector<std::string> constants;
    };
    const RangeLimitCase rangeLimits[] = {
        { "id",{ "-1", "-9223372036854775808", "4294967296", "4294967297", "9223372036854775807" } },
        { "grp",{ "-2147483649", "-9223372036854775808", "2147483648", "9223372036854775807" } },
        { "big",{ "-1", "-9223372036854775808", "9223372036854775807", "9223372036854775808", "18446744073709551615", "18446744073709551616" } },
        { "small",{ "-1", "256", "257", "9223372036854775807" } },
    };
    const char* comparisons[] = { "=", "<>", "<", "<=", ">", ">=" };
    for (const RangeLimitCase& limit : rangeLimits)
    {
        const std::string x = limit.name;
        for (const std::string& constant : limit.constants)
        {
            for (const char* comparison : comparisons)
            {
                tester.check("SELECT id FROM {T} WHERE " + x + " " + comparison + " " + constant + " ORDER BY id", false);
                tester.check("SELECT id FROM {T} WHERE " + constant + " " + comparison + " " + x + " ORDER BY id", false);
            }
            const bool smallValuesExist = x != "big";
            tester.check("SELECT id FROM {T} WHERE " + x + " IN (" + constant + ", 3, 5) ORDER BY id", smallValuesExist);
            tester.check("SELECT id FROM {T} WHERE " + x + " IN (" + constant + ", 3, 5, 9) AND " + x + " > 2 ORDER BY id", smallValuesExist);
            tester.check("SELECT id FROM {T} WHERE " + x + " > " + constant + " AND id < 50 ORDER BY id", false);
            tester.check("SELECT id, " + x + " FROM {T} WHERE " + x + " < " + constant + " ORDER BY " + x + " DESC, id LIMIT 5", false);
        }
    }
    tester.check("SELECT id FROM {T} WHERE small BETWEEN 0 AND 255 ORDER BY id");
    tester.check("SELECT id FROM {T} WHERE small = 250 ORDER BY id");
    tester.check("SELECT id FROM {T} WHERE grp < 2.5 ORDER BY id");
    // Mixed pushed and residual predicates across columns, including strings that stay residual.
    tester.check("SELECT id FROM {T} WHERE id BETWEEN 100 AND 2000 AND grp IN (-7, 3, 50) AND tag <> 't3' ORDER BY id");
    tester.check("SELECT id FROM {T} WHERE id + 0 < 500 AND grp < 0 AND big >= " + bigMid + " ORDER BY id");
}

void runTopNTests(CPushdownTester& tester)
{
    const std::string bigLow = std::to_string(bigBase + 100);
    const std::string bigHigh = std::to_string(bigBase + 4900);
    struct RangeCase
    {
        const char* name;
        std::string low;
        std::string high;
        std::string sparseIn;
    };
    const RangeCase columns[] = {
        { "id", "100", "2900", "3, 17, 400, 2999" },
        { "grp", "-40", "40", "-50, -7, 3, 50" },
        { "big", bigLow, bigHigh, std::to_string(makeRow(0).big) + ", " + std::to_string(makeRow(9).big) },
        { "small", "10", "240", "0, 7, 250" },
    };
    const char* directions[] = { "ASC", "DESC", "ASC NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST" };
    for (const RangeCase& column : columns)
    {
        const std::string x = column.name;
        for (const char* direction : directions)
        {
            // Top-N pushes an OPTIONAL_FILTER wrapping a DYNAMIC_FILTER (or an OR with IS NULL for NULLS FIRST)
            // on the first ORDER BY column; the id tie-breaker keeps results deterministic.
            const std::string order = " ORDER BY " + x + " " + direction + ", id LIMIT 7";
            tester.check("SELECT id, " + x + " FROM {T}" + order);
            tester.check("SELECT id, " + x + " FROM {T}" + order + " OFFSET 11");
            tester.check("SELECT " + x + " FROM {T} ORDER BY " + x + " " + direction + " LIMIT 5");
            tester.check("SELECT id, " + x + " FROM {T} WHERE " + x + " BETWEEN " + column.low + " AND " + column.high + order);
            tester.check("SELECT id, " + x + " FROM {T} WHERE " + x + " < " + column.high + order);
            tester.check("SELECT id, " + x + " FROM {T} WHERE " + x + " IN (" + column.sparseIn + ")" + order);
            tester.check("SELECT id, " + x + " FROM {T} WHERE " + x + " IN (" + column.sparseIn + ") AND " + x + " >= " + column.low + order);
            tester.check("SELECT id, " + x + " FROM {T} WHERE id < 1000 AND grp > -30" + order);
        }
    }
    tester.check("SELECT id FROM {T} ORDER BY id DESC LIMIT 0", false);
    tester.check("SELECT id FROM {T} ORDER BY id DESC LIMIT 5000");
    tester.check("SELECT id, grp FROM {T} WHERE grp = 3 ORDER BY grp DESC, id LIMIT 4");
}

void runJoinTests(CPushdownTester& tester)
{
    // Hash joins push OPTIONAL_FILTER-wrapped min/max, IN and Bloom filters into the probe-side scan at runtime.
    // A self join keeps the build-side key type identical to the probe column so the filters are pushed.
    const char* columns[] = { "id", "grp", "big", "small" };
    for (const char* column : columns)
    {
        const std::string x = column;
        const std::string build = "(SELECT DISTINCT " + x + " AS k FROM {T} WHERE id IN (3, 17, 400, 2999))";
        tester.check("SELECT a.id FROM {T} a JOIN " + build + " b ON a." + x + " = b.k ORDER BY a.id");
        tester.check("SELECT a.id FROM {T} a JOIN " + build + " b ON a." + x + " = b.k WHERE a." + x + " >= (SELECT min(" + x + ") FROM {T}) ORDER BY a.id");
        tester.check("SELECT a.id FROM {T} a JOIN " + build + " b ON a." + x + " = b.k WHERE a.id > 10 ORDER BY a.id");
        tester.check("SELECT a.id FROM {T} a WHERE a." + x + " IN " + build.substr(0, build.size() - 1) + ") ORDER BY a.id");
    }
    tester.check("SELECT a.id FROM {T} a JOIN (SELECT id AS k FROM {T} WHERE id BETWEEN 50 AND 60) b " "ON a.id = b.k WHERE a.id > 55 ORDER BY a.id");
    tester.check("SELECT a.id, b.id FROM {T} a JOIN {T} b ON a.id = b.id + 1 WHERE b.id < 20 ORDER BY a.id");
}

// Nested materialization: rows span several DuckDB chunks and parts, with a variable string, a nested record
// (flattened to columns x and y), a child dataset (including empty ones), a SET OF and fields after variable-length fields.
constexpr unsigned numNestedRows = 5000;

class CTestDirectory
{
    std::string directory;

public:
    CTestDirectory()
    {
        StringBuffer tempPath;
        getSystemTempDir(tempPath);
        std::string tempRoot = tempPath.str();
        if (!tempRoot.empty() && tempRoot.back() != '/' && tempRoot.back() != '\\')
            tempRoot += PATHSEPCHAR;

        std::random_device random;
        for (unsigned attempt = 0; attempt < 16; ++attempt)
        {
            directory = tempRoot + "ws_sql2_pushdown_" + std::to_string(random()) + "_" + std::to_string(random());
            OwnedIFile tempDir = createIFile(directory.c_str());
            if (tempDir->createDirectory())
                return;
        }
        CPPUNIT_ASSERT_MESSAGE("Could not create pushdown test directory", false);
    }

    ~CTestDirectory()
    {
        recursiveRemoveDirectory(directory.c_str());
    }

    const std::string& query() const { return directory; }
};

void writeString(FILE* file, const std::string& value)
{
    writeLittleEndian(file, value.size(), 4);
    CPPUNIT_ASSERT_EQUAL(value.size(), fwrite(value.data(), 1, value.size(), file));
}

void writeDecimal(FILE* file, const std::string& value, unsigned width, unsigned scale, bool isUnsigned)
{
    ::Decimal decimal;
    decimal.setCString(value.c_str());
    const unsigned length = isUnsigned ? (width + 1) / 2 : width / 2 + 1;
    std::vector<byte> packed(length);
    if (isUnsigned)
        decimal.getUDecimal(static_cast<byte>(length), static_cast<byte>(scale), packed.data());
    else
        decimal.getDecimal(static_cast<byte>(length), static_cast<byte>(scale), packed.data());
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(length), fwrite(packed.data(), 1, packed.size(), file));
}

std::string nestedName(unsigned index)
{
    return "n" + std::string(index % 7, 'q');
}

void runDecimalMaterializeTest(CQueryEngine& engine, const std::string& directory)
{
    const std::string path = directory + "/decimal._1_of_1";
    const std::string maxDecimal = std::string(32, '9') + "." + std::string(6, '9');
    const std::string signedLarge = "-" + maxDecimal;
    {
        auto file = openTestFile(path);
        writeDecimal(file.get(), "-123456789012345678.91", 20, 2, false);
        writeDecimal(file.get(), signedLarge, 38, 6, false);
        writeDecimal(file.get(), maxDecimal, 38, 6, true);
        closeTestFile(file);
    }

    const std::string recordEcl = "RECORD decimal20_2 amount; decimal38_6 large; udecimal38_6 unsigned_large; END;";
    const std::string partsJson = "{\"part\":[{\"copy\":[\"" + path + "\"]}]}";
    const std::string optionsJson = "{\"@format\":\"flat\",\"@formatCrc\":\"5\",\"@kind\":\"flat\","
        "\"@logicalName\":\"offline::decimal\"}";
    const std::string sql = "SELECT amount::VARCHAR, large::VARCHAR, unsigned_large::VARCHAR FROM hpcc_scan('" +
        recordEcl + "', '" + partsJson + "', '" + optionsJson + "')";
    std::string result;
    std::string error;
    const bool querySucceeded = engine.executeQuery(sql.c_str(), 0, 0, result, error);
    CPPUNIT_ASSERT_MESSAGE("Decimal materialization query failed: " + error, querySucceeded);
    CPPUNIT_ASSERT_MESSAGE(result, result.find("\"-123456789012345678.91\"") != std::string::npos);
    CPPUNIT_ASSERT_MESSAGE(result, result.find("\"" + signedLarge + "\"") != std::string::npos);
    CPPUNIT_ASSERT_MESSAGE(result, result.find("\"" + maxDecimal + "\"") != std::string::npos);
}

std::string writeNestedParts(const std::string& directory)
{
    const std::string signedAmount = "-123456789012345678.91";
    const std::string unsignedAmount = std::string(32, '9') + "." + std::string(6, '9');
    std::string partsJson = "{\"part\":[";
    for (unsigned part = 0; part < numParts; ++part)
    {
        const std::string path = directory + "/nested._" + std::to_string(part + 1) + "_of_" + std::to_string(numParts);
        auto file = openTestFile(path);
        for (unsigned index = part; index < numNestedRows; index += numParts)
        {
            writeLittleEndian(file.get(), index, 4);
            writeString(file.get(), nestedName(index));
            writeLittleEndian(file.get(), index % 1000, 2);
            CPPUNIT_ASSERT_EQUAL(size_t(3), fwrite("abc", 1, 3, file.get()));
            const unsigned numAccounts = index % 4;
            size_t accountsSize = 0;
            for (unsigned account = 0; account < numAccounts; ++account)
                accountsSize += 4 + 11 + 19 + 4 + account + 1;
            writeLittleEndian(file.get(), accountsSize, 4);
            for (unsigned account = 0; account < numAccounts; ++account)
            {
                writeLittleEndian(file.get(), index * 10 + account, 4);
                writeDecimal(file.get(), signedAmount, 20, 2, false);
                writeDecimal(file.get(), unsignedAmount, 38, 6, true);
                writeString(file.get(), std::string(account + 1, 'x'));
            }
            const unsigned numCodes = index % 3;
            CPPUNIT_ASSERT(fputc(0, file.get()) != EOF);
            writeLittleEndian(file.get(), numCodes * 4, 4);
            for (unsigned code = 0; code < numCodes; ++code)
                writeLittleEndian(file.get(), index + code, 4);
            writeLittleEndian(file.get(), index % 251, 1);
        }
        closeTestFile(file);
        if (part)
            partsJson += ",";
        partsJson += "{\"@tlk\":\"0\",\"copy\":[\"" + path + "\"]}";
    }
    return partsJson + "]}";
}

void runNestedMaterializeTests(CQueryEngine& engine, const std::string& directory)
{
    const std::string partsJson = writeNestedParts(directory);
    const std::string recordEcl = "acct := RECORD unsigned4 a; decimal20_2 signed_value; udecimal38_6 unsigned_value; string b; END; "
        "inner := RECORD unsigned2 x; string3 y; END; "
        "RECORD unsigned4 id; utf8 name; inner n; DATASET(acct) accts; SET OF integer4 codes; unsigned1 tail; END;";
    const std::string optionsJson = "{\"@format\":\"flat\",\"@formatCrc\":\"2\",\"@kind\":\"flat\","
        "\"@logicalName\":\"offline::nested\"}";
    const std::string scan = "hpcc_scan('" + recordEcl + "', '" + partsJson + "', '" + optionsJson + "')";

    uint64_t sumAccounts = 0;
    uint64_t sumNameLength = 0;
    uint64_t sumX = 0;
    uint64_t sumTail = 0;
    uint64_t sumAccountIds = 0;
    uint64_t sumAccountNameLength = 0;
    uint64_t sumCodeCount = 0;
    uint64_t sumCodes = 0;
    unsigned accountGroups[4] = {};
    for (unsigned index = 0; index < numNestedRows; ++index)
    {
        const unsigned numAccounts = index % 4;
        sumAccounts += numAccounts;
        ++accountGroups[numAccounts];
        sumNameLength += nestedName(index).size();
        sumX += index % 1000;
        sumTail += index % 251;
        for (unsigned account = 0; account < numAccounts; ++account)
        {
            sumAccountIds += index * 10 + account;
            sumAccountNameLength += account + 1;
        }
        const unsigned numCodes = index % 3;
        sumCodeCount += numCodes;
        for (unsigned code = 0; code < numCodes; ++code)
            sumCodes += index + code;
    }

    struct NestedCheck
    {
        std::string sql;
        std::string expected;
    };
    const std::vector<NestedCheck> checks = {
        { "SELECT sum(length(name)) FROM " + scan,
         "[[" + std::to_string(sumNameLength) + "]]" },
        { "SELECT count(*), sum(array_length(accts)), sum(length(name)), sum(x), sum(length(y)), sum(tail) FROM " + scan,
         "[[" + std::to_string(numNestedRows) + ", " + std::to_string(sumAccounts) + ", " + std::to_string(sumNameLength) + ", " + std::to_string(sumX) + ", " + std::to_string(numNestedRows * 3) + ", " + std::to_string(sumTail) + "]]" },
        { "SELECT sum(list_sum(list_transform(accts, e -> e.a))), sum(list_sum(list_transform(accts, e -> length(e.b)))) FROM " + scan,
         "[[" + std::to_string(sumAccountIds) + ", " + std::to_string(sumAccountNameLength) + "]]" },
        { "SELECT sum(len(codes)), sum(list_sum(list_transform(codes, c -> c::BIGINT))) FROM " + scan,
         "[[" + std::to_string(sumCodeCount) + ", " + std::to_string(sumCodes) + "]]" },
        { "SELECT array_length(accts) AS a1, count(*) FROM " + scan + " GROUP BY a1 ORDER BY a1",
         "[[0, " + std::to_string(accountGroups[0]) + "],[1, " + std::to_string(accountGroups[1]) + "],[2, " + std::to_string(accountGroups[2]) + "],[3, " + std::to_string(accountGroups[3]) + "]]" },
        { "SELECT id, accts[2].a, accts[2].signed_value::VARCHAR, accts[2].unsigned_value::VARCHAR, accts[2].b, y, tail FROM " + scan + " WHERE id = 4999",
         "[[4999, 49991, \"" + std::string("-123456789012345678.91") + "\", \"" + std::string(32, '9') + "." + std::string(6, '9') + "\", \"xx\", \"abc\", " + std::to_string(4999 % 251) + "]]" },
    };

    for (const auto& check : checks)
    {
        std::string result;
        std::string error;
        const bool querySucceeded = engine.executeQuery(check.sql.c_str(), 0, 0, result, error);
        CPPUNIT_ASSERT_MESSAGE("Nested materialization query failed: " + check.sql + "\n" + error, querySucceeded);
        CPPUNIT_ASSERT_MESSAGE("Nested materialization result mismatch for: " + check.sql + "\nExpected: " + check.expected + "\nResult: " + result.substr(0, 600), result.find(check.expected) != std::string::npos);
    }
}

void runKnownRecordCountOptimizationTest(const std::string& directory)
{
    const std::string missingPart = directory + "/count-shortcut-missing";
    const std::string recordEcl = "RECORD unsigned4 id; END;";
    const std::string partsJson = "{\"part\":[{\"@tlk\":\"0\",\"copy\":[\"" + missingPart + "\"]}]}";
    const std::string optionsJson = "{\"@format\":\"flat\",\"@formatCrc\":\"1\",\"@kind\":\"flat\","
        "\"@logicalName\":\"offline::count-shortcut\"}";
    const char* countSql = "SELECT COUNT(*) FROM hpcc_scan('count-shortcut')";

    CEngineOptions options;
    options.allowLongFormScan = true;
    CQueryEngine engine(nullptr, options);
    CPPUNIT_ASSERT(engine.isInitialized());

    engine.setTestScanMetadata(recordEcl.c_str(), partsJson.c_str(), optionsJson.c_str(), true, 123456789);
    std::string result;
    std::string error;
    std::vector<CQueryResultColumnMetadata> resultMetadata;
    const bool knownCountSucceeded = engine.executeQuery(countSql, 0, 0, result, error, nullptr, nullptr, nullptr, &resultMetadata);
    CPPUNIT_ASSERT_MESSAGE("Known-count query failed without opening its data part: " + error, knownCountSucceeded);
    CPPUNIT_ASSERT_MESSAGE("Known-count query returned an unexpected value: " + result, result.find("[[123456789]]") != std::string::npos);
    CPPUNIT_ASSERT_EQUAL(size_t(1), resultMetadata.size());
    CPPUNIT_ASSERT_EQUAL(std::string("count_star()"), resultMetadata[0].name);

    resultMetadata.clear();
    const bool aliasedCountSucceeded = engine.executeQuery("SELECT COUNT(*) AS total FROM hpcc_scan('count-shortcut')", 0, 0, result, error, nullptr, nullptr, nullptr, &resultMetadata);
    CPPUNIT_ASSERT_MESSAGE("Aliased known-count query failed: " + error, aliasedCountSucceeded);
    CPPUNIT_ASSERT_MESSAGE("Aliased known-count query returned an unexpected value: " + result, result.find("[[123456789]]") != std::string::npos);
    CPPUNIT_ASSERT_EQUAL(size_t(1), resultMetadata.size());
    CPPUNIT_ASSERT_EQUAL(std::string("total"), resultMetadata[0].name);

    const bool filteredCountSucceeded = engine.executeQuery("SELECT COUNT(*) FROM hpcc_scan('count-shortcut') WHERE id > 0", 0, 0, result, error);
    CPPUNIT_ASSERT_MESSAGE("Filtered known-count query unexpectedly skipped its missing data part", !filteredCountSucceeded);

    engine.setTestScanMetadata(recordEcl.c_str(), partsJson.c_str(), optionsJson.c_str(), false, 0);
    const bool unknownCountSucceeded = engine.executeQuery(countSql, 0, 0, result, error);
    CPPUNIT_ASSERT_MESSAGE("Unknown-count query unexpectedly skipped its missing data part", !unknownCountSucceeded);
}

#ifdef _DEBUG
void runScanObjectAuditTest(const std::string& scan)
{
    const unsigned __int64 none = 0;
    CScanObjectAudit audit;
    {
        CEngineOptions options;
        options.allowLongFormScan = true;
        CQueryEngine engine(nullptr, options);
        audit = engine.queryScanObjectAudit();
        CPPUNIT_ASSERT(audit.isValid());

        std::string result;
        std::string error;
        const bool querySucceeded = engine.executeQuery(("SELECT count(*) FROM " + scan).c_str(), 0, 0, result, error);
        CPPUNIT_ASSERT_MESSAGE("Audited scan failed: " + error, querySucceeded);
        CPPUNIT_ASSERT(audit.queryCreated().bindData >= 1);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("A completed query left hpcc_scan objects alive", none, audit.queryLive().total());

        bool complete = false;
        const bool cursorStarted = engine.startCursorQuery(("SELECT id FROM " + scan).c_str(), false, nullptr, error);
        CPPUNIT_ASSERT_MESSAGE("Audited cursor failed: " + error, cursorStarted);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(10, 0, result, complete, error));
        CPPUNIT_ASSERT(!complete);
        const CEngineScanObjectCounts live = audit.queryLive();
        CPPUNIT_ASSERT(live.bindData >= 1);
        CPPUNIT_ASSERT(live.globalStates >= 1);
        // Local states belong to pipeline tasks, which may already have finished for a small file.
        CPPUNIT_ASSERT(audit.queryCreated().localStates >= 2);
    }
    const CEngineScanObjectCounts created = audit.queryCreated();
    CPPUNIT_ASSERT(created.bindData >= 2);
    CPPUNIT_ASSERT(created.globalStates >= 2);
    CPPUNIT_ASSERT(created.localStates >= 2);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Destroying the engine left hpcc_scan objects alive", none, audit.queryLive().total());
}
#endif

// Measures how long an interrupt takes to stop a cursor's first fetch while hpcc_scan is reading. hpcc_scan has no
// interrupt check of its own, so the query stops at DuckDB's next chunk or task boundary.
void runScanInterruptLatencyTest(const std::string& directory)
{
    constexpr unsigned rowsPerPart = 100000;
    constexpr unsigned numRepeatedParts = 5000;
    const std::string path = directory + "/interrupt._1_of_1";
    {
        auto file = openTestFile(path);
        for (unsigned index = 0; index < rowsPerPart; ++index)
            writeLittleEndian(file.get(), index, 4);
        closeTestFile(file);
    }
    // Repeating one physical part keeps the scan long-running without a large test file.
    std::string partsJson = "{\"part\":[";
    for (unsigned part = 0; part < numRepeatedParts; ++part)
    {
        if (part)
            partsJson += ",";
        partsJson += "{\"@tlk\":\"0\",\"copy\":[\"" + path + "\"]}";
    }
    partsJson += "]}";
    const std::string optionsJson = "{\"@format\":\"flat\",\"@formatCrc\":\"3\",\"@kind\":\"flat\","
        "\"@logicalName\":\"offline::interrupt\"}";
    const std::string sql = "SELECT sum(id) FROM hpcc_scan('{ unsigned4 id };', '" + partsJson + "', '" + optionsJson + "')";

    CEngineOptions options;
    options.allowLongFormScan = true;
    CQueryEngine engine(nullptr, options);
    std::string rows;
    std::string error;
    bool complete = false;
    const bool started = engine.startCursorQuery(sql.c_str(), false, nullptr, error);
    CPPUNIT_ASSERT_MESSAGE("Interrupt latency query failed to start: " + error, started);

    std::atomic<cycle_t> interruptedAt{0};
    std::thread interrupter([&engine, &interruptedAt]()
    {
        MilliSleep(200);
        interruptedAt = get_cycles_now();
        engine.interrupt();
    });
    const bool fetched = engine.fetchCursorBatch(1, 0, rows, complete, error);
    const cycle_t stoppedAt = get_cycles_now();
    interrupter.join();
    CPPUNIT_ASSERT_MESSAGE("Interrupt latency query completed before it was interrupted", !fetched);
    CPPUNIT_ASSERT_MESSAGE("Interrupt latency query returned an unexpected error: " + error, error.find("interrupted") != std::string::npos);
    const unsigned latencyMs = static_cast<unsigned>(cycle_to_nanosec(stoppedAt - interruptedAt) / 1000000);
    DBGLOG("WsSQL2: hpcc_scan interrupt latency %ums", latencyMs);
    CPPUNIT_ASSERT_MESSAGE("hpcc_scan took " + std::to_string(latencyMs) + "ms to stop after an interrupt", latencyMs < 2000);
}

} // namespace

void runWsSql2PushdownTestSuite()
{
    CTestDirectory directory;
    runKnownRecordCountOptimizationTest(directory.query());
    const std::string partsJson = writeParts(directory.query());
    const std::string recordEcl = "{ unsigned4 id, integer4 grp, unsigned8 big, string4 tag, unsigned1 small, utf8 name };";
    const std::string optionsJson = "{\"@format\":\"flat\",\"@formatCrc\":\"1\",\"@kind\":\"flat\","
        "\"@logicalName\":\"offline::pushdown\"}";
    const std::string scan = "hpcc_scan('" + recordEcl + "', '" + partsJson + "', '" + optionsJson + "')";
#ifdef _DEBUG
    runScanObjectAuditTest(scan);
#endif
    runScanInterruptLatencyTest(directory.query());

    {
        CEngineOptions options;
        options.allowLongFormScan = true;
        CQueryEngine engine(nullptr, options);
        std::string result;
        std::string error;
        const bool querySucceeded = engine.executeQuery(("SELECT count(*), min(grp), max(big) FROM " + scan).c_str(), 0, 0, result, error);
        CPPUNIT_ASSERT_MESSAGE("Offline pushdown scan failed: " + error, querySucceeded);
        CPPUNIT_ASSERT_MESSAGE("Offline pushdown scan returned unexpected data: " + result, result.find("[[3000, -50, ") != std::string::npos);

        CPushdownTester tester(engine, scan);
        tester.check("SELECT id, big FROM {T} WHERE big > 9223372036854775807 ORDER BY id");
        // The differential tests rely on the scan itself pushing filters while the LIMIT-wrapped oracle does not.
        std::string pushedPlan;
        std::string oraclePlan;
        std::string planJson;
        StringArray projected;
        StringArray residual;
        std::vector<CQueryResultColumnMetadata> resultMetadata;
        const bool pushedExplainSucceeded = engine.explainQuery(("SELECT id FROM " + scan + " WHERE id < 5").c_str(), pushedPlan, planJson, projected, residual, resultMetadata, error);
        CPPUNIT_ASSERT_MESSAGE("Could not explain pushed query: " + error, pushedExplainSucceeded);
        const bool oracleExplainSucceeded = engine.explainQuery(("SELECT id FROM (SELECT * FROM " + scan + " LIMIT 1000000000) WHERE id < 5").c_str(), oraclePlan, planJson, projected, residual, resultMetadata, error);
        CPPUNIT_ASSERT_MESSAGE("Could not explain oracle query: " + error, oracleExplainSucceeded);
        CPPUNIT_ASSERT_MESSAGE("Pushdown oracle is not isolated from scan filters:\n" + pushedPlan + "\n" + oraclePlan, pushedPlan.find("Filters:") != std::string::npos && oraclePlan.find("Filters:") == std::string::npos);

        runPredicateTests(tester);
        tester.checkPushed("SELECT id FROM {T} WHERE 100 < id AND id < 200 ORDER BY id", "id");
        tester.checkPushed("SELECT id FROM {T} WHERE id = 3 OR id = 17 ORDER BY id", "id");
        tester.checkPushed("SELECT id FROM {T} WHERE id < 500 AND id % 2 = 0 ORDER BY id", "id");
        tester.checkResidual("SELECT id FROM {T} WHERE id < 500 AND id % 2 = 0 ORDER BY id", "id % 2");
        tester.checkResidual("SELECT id FROM {T} WHERE tag = 't3' ORDER BY id", "tag", false);
        tester.checkPushed("SELECT id FROM {T} WHERE name = 'n' ORDER BY id", "name");
        // HPCC string ordering differs from DuckDB (padding/collation), so string equality is pushed as a superset
        // with the predicate retained, and string ranges are not pushed at all.
        tester.checkResidual("SELECT id FROM {T} WHERE name = 'n' ORDER BY id", "name");
        tester.checkResidual("SELECT id FROM {T} WHERE name < 'nqq' ORDER BY id", "name");
        tester.check("SELECT id FROM {T} WHERE name = '' ORDER BY id", false);
        tester.checkPushed("SELECT id FROM {T} WHERE name IN ('n', 'nqq') ORDER BY id", "name");
        tester.checkPushed("SELECT id FROM {T} WHERE name = 'n' OR name = 'nqq' ORDER BY id", "name");
        tester.checkResidual("SELECT id FROM {T} WHERE name = 'n' OR id = 17 ORDER BY id", "name");
        tester.checkPushed("SELECT id FROM {T} WHERE tag = 't3  ' ORDER BY id", "tag");
        runTopNTests(tester);
        runJoinTests(tester);
        runDecimalMaterializeTest(engine, directory.query());
        runNestedMaterializeTests(engine, directory.query());
    }
}
