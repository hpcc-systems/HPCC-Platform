#include "optimizer.hpp"

#include "jlib.hpp"
#include "thorcommon.hpp"

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstring>
#include <limits>

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#endif

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb::hpcc
{
namespace
{
// HPCC STRING/VARSTRING comparisons ignore trailing spaces and UTF8 comparisons use locale collation, unlike DuckDB
// VARCHAR. Filters on these types are only pushed as equality supersets, and DuckDB must still evaluate the original
// comparison on the rows that are returned.
static bool isStringFilterType(const RtlTypeInfo* type)
{
    switch (type->getType())
    {
    case type_string:
    case type_varstring:
    case type_utf8:
        return true;
    default:
        return false;
    }
}

static std::string joinDescriptions(const std::vector<std::string>& descriptions)
{
    std::string result;
    for (const auto& description : descriptions)
    {
        if (!result.empty())
            result.push_back('\n');
        result.append(description);
    }
    return result;
}

// IFieldFilter does not expose complete range bounds or filter equality, so these helpers extract that information
// from its serialized form until the filter API provides direct access.
static std::string extractFieldFilterText(const IFieldFilter& filter)
{
    StringBuffer serialized;
    filter.serialize(serialized);
    return serialized.str();
}

static std::string extractFieldFilterDescription(const IFieldFilter& filter, const std::string& fieldName)
{
    const std::string serialized = extractFieldFilterText(filter);
    const char* comparison = strchr(serialized.c_str(), '=');
    return fieldName + (comparison ? comparison : serialized.c_str());
}

static IValueSet* extractFieldFilterValues(const IFieldFilter& filter)
{
    MemoryBuffer serialized;
    filter.serialize(serialized);
    unsigned fieldIndex;
    serialized.readPacked(fieldIndex);
    char kind;
    serialized.read(kind);
    if (kind != '=')
        return nullptr;
    const RtlTypeInfo& type = filter.queryType();
    const size32_t setStart = serialized.getPos();
    unsigned numTransitions;
    serialized.readPacked(numTransitions);
    if (numTransitions == 1)
    {
        // A single-value filter is serialized as one raw value; value sets always hold transition pairs.
        const size32_t size = type.size(serialized.readDirect(0), nullptr);
        const void* value = serialized.readDirect(size);
        Owned<IValueSet> values = createValueSet(type);
        values->addRawRange(value, value);
        return values.getClear();
    }
    serialized.reset(setStart);
    return createValueSet(type, serialized);
}

static bool haveSameFilterType(const IFieldFilter& left, const IFieldFilter& right)
{
    const RtlTypeInfo& leftType = left.queryType();
    const RtlTypeInfo& rightType = right.queryType();
    return leftType.fieldType == rightType.fieldType && leftType.length == rightType.length;
}

static unsigned findActualField(const CScanBindData& bindData, unsigned expectedIndex)
{
    if (!bindData.actualMetadata || expectedIndex >= bindData.fields.size())
        return static_cast<unsigned>(-1);
    const RtlRecord& actualRecord = bindData.actualMetadata->queryRecordAccessor(true);
    const char* fieldPath = bindData.fields[expectedIndex].fieldPath.c_str();
    for (unsigned actualIndex = 0; actualIndex < actualRecord.getNumFields(); ++actualIndex)
    {
        if (streq(actualRecord.queryName(actualIndex), fieldPath))
            return actualIndex;
    }
    return static_cast<unsigned>(-1);
}

static bool supportsHpccScanPushdownTypeInternal(const duckdb::FunctionData& functionData, duckdb::idx_t fieldIndex)
{
    const auto& bindData = functionData.Cast<CScanBindData>();
    if (!bindData.expectedRecord || fieldIndex >= bindData.expectedRecord->record.getNumFields())
        return false;
    const RtlTypeInfo* type = queryFilterValueType(bindData.expectedRecord->record.queryType(static_cast<unsigned>(fieldIndex)));
    switch (type->getType())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
    case type_keyedint:
        return true;
    // String comparison semantics differ from DuckDB VARCHAR (see isStringFilterType), so DuckDB must keep these
    // filters; equality is still pushed as a superset through pushdown_complex_filter.
    default:
        return false;
    }
}

static void recordHpccScanFilterDecisions(duckdb::LogicalGet& get)
{
    auto& bindData = get.bind_data->Cast<CScanBindData>();
    bindData.keyedFilterDescriptions.clear();
    bindData.postFilterDescriptions.clear();
    bindData.filterLayoutTranslated = false;
    bindData.readerProjectionDescriptions.clear();
    const auto& columnIds = get.GetColumnIds();
    if (get.projection_ids.empty())
    {
        for (const auto& columnId : columnIds)
        {
            const unsigned fieldIndex = static_cast<unsigned>(columnId.GetPrimaryIndex());
            if (fieldIndex < bindData.fields.size())
                bindData.readerProjectionDescriptions.push_back(bindData.fields[fieldIndex].fieldPath);
        }
    }
    else
    {
        for (duckdb::idx_t projection : get.projection_ids)
        {
            if (projection >= columnIds.size())
                continue;
            const unsigned fieldIndex = static_cast<unsigned>(columnIds[projection].GetPrimaryIndex());
            if (fieldIndex < bindData.fields.size())
                bindData.readerProjectionDescriptions.push_back(bindData.fields[fieldIndex].fieldPath);
        }
    }
    if (!bindData.actualMetadata)
        return;

    const RtlRecord& actualRecord = bindData.actualMetadata->queryRecordAccessor(true);
    if (bindData.expectedRecord)
    {
        try
        {
            Owned<const IKeyTranslator> translator = createKeyTranslator(actualRecord, bindData.expectedRecord->record);
            bindData.filterLayoutTranslated = translator && translator->needsTranslate();
        }
        catch (IException* exception)
        {
            exception->Release();
        }
    }
    const unsigned keyedFields = actualRecord.getNumKeyedFields();
    std::vector<bool> constrained(keyedFields, false);
    for (const auto& entry : get.table_filters.filters)
    {
        if (entry.first >= bindData.fields.size())
            continue;
        const unsigned expectedIndex = static_cast<unsigned>(entry.first);
        if (!supportsHpccScanPushdownTypeInternal(bindData, expectedIndex))
            continue;
        const unsigned actualIndex = findActualField(bindData, expectedIndex);
        if (actualIndex < keyedFields)
            constrained[actualIndex] = true;
    }
    ForEachItemIn(pushedIndex, bindData.pushedFilters)
    {
        const unsigned expectedIndex = bindData.pushedFilters.item(pushedIndex).queryFieldIndex();
        const unsigned actualIndex = findActualField(bindData, expectedIndex);
        if (actualIndex < keyedFields)
            constrained[actualIndex] = true;
    }
    for (const auto& entry : get.table_filters.filters)
    {
        if (entry.first >= bindData.fields.size())
            continue;
        const unsigned expectedIndex = static_cast<unsigned>(entry.first);
        if (!supportsHpccScanPushdownTypeInternal(bindData, expectedIndex))
            continue;
        const unsigned actualIndex = findActualField(bindData, expectedIndex);
        if (actualIndex == static_cast<unsigned>(-1))
            continue;
        bool contiguousKeyed = actualIndex < keyedFields;
        for (unsigned preceding = 0; contiguousKeyed && preceding < actualIndex; ++preceding)
            contiguousKeyed = constrained[preceding];
        const std::string description = entry.second->ToString(bindData.fields[expectedIndex].fieldPath);
        auto& targetFilter = contiguousKeyed ? bindData.keyedFilterDescriptions : bindData.postFilterDescriptions;
        targetFilter.push_back(description);
    }
    ForEachItemIn(decisionIndex, bindData.pushedFilters)
    {
        const IFieldFilter& filter = bindData.pushedFilters.item(decisionIndex);
        const unsigned expectedIndex = filter.queryFieldIndex();
        const unsigned actualIndex = findActualField(bindData, expectedIndex);
        if (actualIndex == static_cast<unsigned>(-1) || expectedIndex >= bindData.fields.size())
            continue;
        bool contiguousKeyed = actualIndex < keyedFields;
        for (unsigned preceding = 0; contiguousKeyed && preceding < actualIndex; ++preceding)
            contiguousKeyed = constrained[preceding];
        const std::string description = extractFieldFilterDescription(filter, bindData.fields[expectedIndex].fieldPath);
        auto& targetFilter = contiguousKeyed ? bindData.keyedFilterDescriptions : bindData.postFilterDescriptions;
        targetFilter.push_back(description);
    }
}

static CScanBindData* findHpccScanThroughProjections(duckdb::LogicalOperator& operation)
{
    duckdb::LogicalOperator* current = &operation;
    while (current->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION)
    {
        if (current->children.size() != 1)
            return nullptr;
        current = current->children[0].get();
    }
    if (current->type != duckdb::LogicalOperatorType::LOGICAL_GET)
        return nullptr;
    auto& get = current->Cast<duckdb::LogicalGet>();
    if (get.function.name != "hpcc_scan" || !get.bind_data)
        return nullptr;
    return &get.bind_data->Cast<CScanBindData>();
}

static void recordHpccScanFilters(duckdb::LogicalOperator& operation)
{
    if (operation.type == duckdb::LogicalOperatorType::LOGICAL_GET)
    {
        auto& get = operation.Cast<duckdb::LogicalGet>();
        if (get.function.name == "hpcc_scan" && get.bind_data)
        {
            get.bind_data->Cast<CScanBindData>().residualFilterDescriptions.clear();
            recordHpccScanFilterDecisions(get);
        }
    }
    for (auto& child : operation.children)
        recordHpccScanFilters(*child);
    if (operation.type == duckdb::LogicalOperatorType::LOGICAL_FILTER && operation.children.size() == 1)
    {
        CScanBindData* bindData = findHpccScanThroughProjections(*operation.children[0]);
        if (bindData)
        {
            for (const auto& expression : operation.expressions)
                bindData->residualFilterDescriptions.push_back(expression->ToString());
        }
    }
}

static void pushdownHpccScanLimit(duckdb::unique_ptr<duckdb::LogicalOperator>& operation)
{
    if (operation->type != duckdb::LogicalOperatorType::LOGICAL_LIMIT)
    {
        for (auto& child : operation->children)
            pushdownHpccScanLimit(child);
        return;
    }

    auto& limitOperation = operation->Cast<duckdb::LogicalLimit>();
    duckdb::reference<duckdb::LogicalOperator> child = *operation->children[0];
    while (child.get().type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION)
    {
        if (child.get().children.empty())
            return;
        child = *child.get().children[0];
    }
    if (child.get().type != duckdb::LogicalOperatorType::LOGICAL_GET)
        return;

    auto& get = child.get().Cast<duckdb::LogicalGet>();
    if (get.function.name != "hpcc_scan" || !get.bind_data)
        return;
    if (limitOperation.limit_val.Type() != duckdb::LimitNodeType::CONSTANT_VALUE && limitOperation.limit_val.Type() != duckdb::LimitNodeType::UNSET)
        return;
    if (limitOperation.offset_val.Type() != duckdb::LimitNodeType::CONSTANT_VALUE && limitOperation.offset_val.Type() != duckdb::LimitNodeType::UNSET)
        return;

    auto& bindData = get.bind_data->Cast<CScanBindData>();
    // Parallel part readers cannot share a scan-level row budget, so DuckDB keeps LIMIT/OFFSET.
    if (bindData.scanParts > 1)
        return;
    // DuckDB re-plans unsupported table filters as a FILTER above the scan, which must see rows before the LIMIT.
    // LogicalGet table filters are keyed by the absolute field index, not by position in GetColumnIds().
    for (const auto& entry : get.table_filters.filters)
    {
        if (!supportsHpccScanPushdownTypeInternal(bindData, entry.first))
            return;
    }
    bindData.limit = limitOperation.limit_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE ? static_cast<int64_t>(limitOperation.limit_val.GetConstantValue()) : -1;
    bindData.offset = limitOperation.offset_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE ? static_cast<int64_t>(limitOperation.offset_val.GetConstantValue()) : 0;
    operation = std::move(operation->children[0]);
}

static bool rewriteKnownHpccCount(duckdb::unique_ptr<duckdb::LogicalOperator>& operation)
{
    if (operation->type == duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY)
    {
        auto& aggregate = operation->Cast<duckdb::LogicalAggregate>();
        if (aggregate.children.size() == 1 && aggregate.groups.empty() && aggregate.grouping_sets.empty() &&
            aggregate.grouping_functions.empty() && aggregate.expressions.size() == 1)
        {
            const auto* count = dynamic_cast<const duckdb::BoundAggregateExpression*>(aggregate.expressions.front().get());
            auto& child = *aggregate.children.front();
            if (count && count->function.name == "count_star" && count->children.empty() && !count->filter &&
                !count->order_bys && !count->IsDistinct() && count->return_type == duckdb::LogicalType::BIGINT &&
                child.type == duckdb::LogicalOperatorType::LOGICAL_GET)
            {
                auto& scan = child.Cast<duckdb::LogicalGet>();
                if (scan.function.name == "hpcc_scan" && scan.bind_data && scan.table_filters.filters.empty() && !scan.dynamic_filters)
                {
                    const auto& bindData = scan.bind_data->Cast<CScanBindData>();
                    if (bindData.recordCount && bindData.pushedFilters.ordinality() == 0 && bindData.limit < 0 &&
                        bindData.offset == 0 &&
                        *bindData.recordCount <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
                    {
                        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
                        auto value = duckdb::make_uniq<duckdb::BoundConstantExpression>(
                            duckdb::Value::BIGINT(static_cast<int64_t>(*bindData.recordCount)));
                        value->alias = count->alias.empty() ? "count_star()" : count->alias;
                        expressions.push_back(std::move(value));
                        auto replacement = duckdb::make_uniq<duckdb::LogicalProjection>(aggregate.aggregate_index, std::move(expressions));
                        replacement->children.push_back(duckdb::make_uniq<duckdb::LogicalDummyScan>(aggregate.group_index));
                        replacement->ResolveOperatorTypes();
                        operation = std::move(replacement);
                        return true;
                    }
                }
            }
        }
    }

    for (auto& child : operation->children)
        rewriteKnownHpccCount(child);
    return false;
}

static void optimizeHpccScan(duckdb::OptimizerExtensionInput&, duckdb::unique_ptr<duckdb::LogicalOperator>& plan)
{
    rewriteKnownHpccCount(plan);
    pushdownHpccScanLimit(plan);
    recordHpccScanFilters(*plan);
}

static duckdb::unique_ptr<duckdb::NodeStatistics> estimateHpccScanCardinality(duckdb::ClientContext&, const duckdb::FunctionData* functionData)
{
    const auto& recordCount = functionData->Cast<CScanBindData>().recordCount;
    if (!recordCount)
        return nullptr;
    return duckdb::make_uniq<duckdb::NodeStatistics>(static_cast<duckdb::idx_t>(*recordCount));
}

static duckdb::InsertionOrderPreservingMap<std::string> describeHpccScan(duckdb::TableFunctionToStringInput& input)
{
    duckdb::InsertionOrderPreservingMap<std::string> description;
    const auto& bindData = input.bind_data->Cast<CScanBindData>();
    description["Physical Parts"] = std::to_string(bindData.offlinePartCopies.size());
    if (bindData.isKey)
        description["Key Data Parts"] = std::to_string(bindData.scanParts);
    description["Max Threads"] = std::to_string(std::max(bindData.scanParts, 1U));
    description["Partitioned Scan"] = bindData.scanParts > 1 ? "true" : "false";
    if (bindData.offlineAccessOptions.formatOptions && bindData.offlineAccessOptions.formatOptions->getPropBool("@grouped", false))
        description["Grouped"] = "true (group boundaries flattened)";
    description["Limit Pushdown"] = bindData.limit >= 0 ? "true" : "false";
    if (bindData.limit >= 0)
        description["Pushed Limit"] = std::to_string(bindData.limit);
    if (bindData.offset > 0)
        description["Pushed Offset"] = std::to_string(bindData.offset);
    if (!bindData.keyedFilterDescriptions.empty())
        description["Keyed Filters"] = joinDescriptions(bindData.keyedFilterDescriptions);
    if (!bindData.postFilterDescriptions.empty())
        description["Post Filters"] = joinDescriptions(bindData.postFilterDescriptions);
    if (!bindData.residualFilterDescriptions.empty())
        description["Residual Filters"] = joinDescriptions(bindData.residualFilterDescriptions);
    if (bindData.filterLayoutTranslated)
        description["Filter Layout Translation"] = "expected-to-actual";
    if (!bindData.readerProjectionDescriptions.empty())
        description["Reader Projection"] = joinDescriptions(bindData.readerProjectionDescriptions);
    return description;
}

static bool isIntegerConstantType(duckdb::LogicalTypeId typeId)
{
    switch (typeId)
    {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
        return true;
    default:
        return false;
    }
}

static bool formatFilterValue(const duckdb::Value& constant, std::string& value)
{
    if (!isIntegerConstantType(constant.type().id()))
        return false;
    value = constant.ToString();
    return true;
}

// Builds an inclusive single-value range directly from transitions: serializing through the range syntax would turn
// an empty string into an unbounded range.
static bool buildStringEqualityValueSet(unsigned fieldIndex, const duckdb::Value& constant, const RtlRecord& record, Owned<IValueSet>& values)
{
    if (constant.type().id() != duckdb::LogicalTypeId::VARCHAR)
        return false;
    const RtlTypeInfo* fieldType = queryFilterValueType(record.queryType(fieldIndex));
    const std::string& text = duckdb::StringValue::Get(constant);
    if (fieldType->getType() == type_string && fieldType->isFixedSize() && text.size() != fieldType->length)
        return false;
    values.setown(createValueSet(*record.queryType(fieldIndex)));
    const size32_t length = rtlUtf8Length(static_cast<unsigned>(text.size()), text.data());
    Owned<IValueTransition> lower = values->createUtf8Transition(CMPge, length, text.data(), MatchFullString);
    Owned<IValueTransition> upper = values->createUtf8Transition(CMPle, length, text.data(), MatchFullString);
    values->addRange(lower, upper);
    return true;
}

// Returns -1 or 1 when an integer constant lies below or above the range of an HPCC integer field, otherwise 0.
static int compareToIntegerFieldRange(const RtlTypeInfo* fieldType, const duckdb::Value& constant)
{
    switch (fieldType->getType())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
        break;
    default:
        return 0;
    }
    const auto typeId = constant.type().id();
    if (!isIntegerConstantType(typeId))
        return 0;
    if (fieldType->length == 0 || fieldType->length > sizeof(int64_t))
        return 0;
    const unsigned bits = fieldType->length * 8;
    if (typeId == duckdb::LogicalTypeId::UBIGINT)
    {
        const uint64_t number = constant.GetValue<uint64_t>();
        if (fieldType->isSigned())
            return number > static_cast<uint64_t>(INT64_MAX) >> (64 - bits) ? 1 : 0;
        if (bits < 64 && number >= (static_cast<uint64_t>(1) << bits))
            return 1;
        return 0;
    }
    const int64_t number = constant.GetValue<int64_t>();
    if (fieldType->isSigned())
    {
        if (bits >= 64)
            return 0;
        const int64_t limit = static_cast<int64_t>(1) << (bits - 1);
        return number < -limit ? -1 : (number >= limit ? 1 : 0);
    }
    if (number < 0)
        return -1;
    if (bits < 64 && static_cast<uint64_t>(number) >= (static_cast<uint64_t>(1) << bits))
        return 1;
    return 0;
}

static bool buildValueSet(unsigned fieldIndex, duckdb::ExpressionType comparisonType, const duckdb::Value& constant, const RtlRecord& record, Owned<IValueSet>& values)
{
    if (fieldIndex >= record.getNumFields())
        return false;
    // An out-of-range constant would wrap when deserialized into the field's type (e.g. -1 as an unsigned),
    // so resolve the comparison to an empty or complete value set instead.
    const int outOfRange = compareToIntegerFieldRange(queryFilterValueType(record.queryType(fieldIndex)), constant);
    if (outOfRange)
    {
        bool matchesAll = false;
        switch (comparisonType)
        {
        case duckdb::ExpressionType::COMPARE_EQUAL:
            matchesAll = false;
            break;
        case duckdb::ExpressionType::COMPARE_NOTEQUAL:
            matchesAll = true;
            break;
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
            matchesAll = outOfRange > 0;
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            matchesAll = outOfRange < 0;
            break;
        default:
            return false;
        }
        values.setown(createValueSet(*record.queryType(fieldIndex)));
        if (matchesAll)
            values->invertSet();
        return true;
    }
    if (isStringFilterType(queryFilterValueType(record.queryType(fieldIndex))))
    {
        if (comparisonType != duckdb::ExpressionType::COMPARE_EQUAL)
            return false;
        return buildStringEqualityValueSet(fieldIndex, constant, record, values);
    }
    std::string value;
    if (!formatFilterValue(constant, value))
        return false;
    std::string expression;
    bool invert = false;
    switch (comparisonType)
    {
    case duckdb::ExpressionType::COMPARE_EQUAL:
        expression = "[" + value + "]";
        break;
    case duckdb::ExpressionType::COMPARE_NOTEQUAL:
        expression = "[" + value + "]";
        invert = true;
        break;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
        expression = "(," + value + ")";
        break;
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        expression = "(," + value + "]";
        break;
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        expression = "(" + value + ",)";
        break;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        expression = "[" + value + ",)";
        break;
    default:
        return false;
    }
    values.setown(createValueSet(*record.queryType(fieldIndex)));
    deserializeSet(*values, expression.c_str());
    if (invert)
        values->invertSet();
    return true;
}

static bool buildScalarFilter(unsigned fieldIndex, duckdb::ExpressionType comparisonType, const duckdb::Value& constant, const RtlRecord& record, Owned<IFieldFilter>& filter)
{
    Owned<IValueSet> values;
    if (!buildValueSet(fieldIndex, comparisonType, constant, record, values))
        return false;
    filter.setown(createFieldFilter(fieldIndex, values));
    return filter != nullptr;
}

static bool buildScalarFilter(const duckdb::BoundComparisonExpression& comparison, const duckdb::LogicalGet& get, const RtlRecord& record, Owned<IFieldFilter>& filter, bool& exact)
{
    const duckdb::Expression* columnExpression = comparison.left.get();
    const duckdb::Expression* constantExpression = comparison.right.get();
    duckdb::ExpressionType comparisonType = comparison.type;
    if (dynamic_cast<const duckdb::BoundConstantExpression*>(columnExpression))
    {
        std::swap(columnExpression, constantExpression);
        switch (comparisonType)
        {
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
            comparisonType = duckdb::ExpressionType::COMPARE_GREATERTHAN;
            break;
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
            comparisonType = duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
            comparisonType = duckdb::ExpressionType::COMPARE_LESSTHAN;
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            comparisonType = duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
            break;
        default:
            break;
        }
    }
    const auto* constant = dynamic_cast<const duckdb::BoundConstantExpression*>(constantExpression);
    if (!constant)
        return false;
    duckdb::idx_t projectionIndex = duckdb::DConstants::INVALID_INDEX;
    if (const auto* reference = dynamic_cast<const duckdb::BoundReferenceExpression*>(columnExpression))
        projectionIndex = reference->index;
    else if (const auto* column = dynamic_cast<const duckdb::BoundColumnRefExpression*>(columnExpression))
        projectionIndex = column->binding.column_index;
    if (projectionIndex >= get.GetColumnIds().size())
        return false;
    const unsigned fieldIndex = static_cast<unsigned>(get.GetColumnIds()[projectionIndex].GetPrimaryIndex());
    if (fieldIndex >= record.getNumFields())
        return false;
    exact = !isStringFilterType(queryFilterValueType(record.queryType(fieldIndex)));
    return buildScalarFilter(fieldIndex, comparisonType, constant->value, record, filter);
}

// Returns true if a filter was staged; exact is cleared when the original expression must also be kept as a residual.
static bool tryBuildScalarFilter(const duckdb::Expression& expression, const duckdb::LogicalGet& get, const RtlRecord& record, FieldFilterArray& staged, bool& exact)
{
    const auto* comparison = dynamic_cast<const duckdb::BoundComparisonExpression*>(&expression);
    if (!comparison)
        return false;

    Owned<IFieldFilter> filter;
    try
    {
        if (!buildScalarFilter(*comparison, get, record, filter, exact))
            return false;
    }
    catch (IException* exception)
    {
        exception->Release();
        return false;
    }
    staged.append(*filter.getClear());
    return true;
}

static duckdb::unique_ptr<duckdb::Expression> pushdownAndChild(duckdb::unique_ptr<duckdb::Expression> expression, const duckdb::LogicalGet& get, const RtlRecord& record, FieldFilterArray& staged)
{
    auto* conjunction = dynamic_cast<duckdb::BoundConjunctionExpression*>(expression.get());
    if (!conjunction || conjunction->type != duckdb::ExpressionType::CONJUNCTION_AND)
    {
        bool exact = true;
        if (tryBuildScalarFilter(*expression, get, record, staged, exact) && exact)
            return nullptr;
        return expression;
    }

    std::vector<duckdb::unique_ptr<duckdb::Expression>> residualChildren;
    for (auto& child : conjunction->children)
    {
        auto residual = pushdownAndChild(std::move(child), get, record, staged);
        if (residual)
            residualChildren.push_back(std::move(residual));
    }
    if (residualChildren.empty())
        return nullptr;
    if (residualChildren.size() == 1)
        return std::move(residualChildren.front());

    auto residual = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND);
    residual->children = std::move(residualChildren);
    return duckdb::unique_ptr_cast<duckdb::BoundConjunctionExpression, duckdb::Expression>(std::move(residual));
}

// Builds the value set a pushed table filter allows. A null result means the filter imposes no mandatory constraint.
static bool buildTableFilterValues(const duckdb::TableFilter& tableFilter, unsigned fieldIndex, const RtlRecord& record, Owned<IValueSet>& values)
{
    switch (tableFilter.filter_type)
    {
    case duckdb::TableFilterType::OPTIONAL_FILTER:
        // Advisory filters (Top-N, join and non-dense IN filters) are not required for correctness.
        return true;
    case duckdb::TableFilterType::CONSTANT_COMPARISON:
    {
        const auto& constantFilter = tableFilter.Cast<duckdb::ConstantFilter>();
        return buildValueSet(fieldIndex, constantFilter.comparison_type, constantFilter.constant, record, values);
    }
    case duckdb::TableFilterType::IN_FILTER:
    {
        const auto& inFilter = tableFilter.Cast<duckdb::InFilter>();
        for (const auto& value : inFilter.values)
        {
            Owned<IValueSet> itemValues;
            if (!buildValueSet(fieldIndex, duckdb::ExpressionType::COMPARE_EQUAL, value, record, itemValues))
                return false;
            if (values)
                values->unionSet(itemValues);
            else
                values.setown(itemValues.getClear());
        }
        return values != nullptr;
    }
    case duckdb::TableFilterType::CONJUNCTION_AND:
    {
        // Static and runtime (join) filters on the same column can arrive as nested conjunctions.
        const auto& conjunction = tableFilter.Cast<duckdb::ConjunctionAndFilter>();
        for (const auto& child : conjunction.child_filters)
        {
            Owned<IValueSet> childValues;
            if (!buildTableFilterValues(*child, fieldIndex, record, childValues))
                return false;
            if (!childValues)
                continue;
            if (values)
                values->intersectSet(childValues);
            else
                values.setown(childValues.getClear());
        }
        return true;
    }
    case duckdb::TableFilterType::CONJUNCTION_OR:
    {
        const auto& conjunction = tableFilter.Cast<duckdb::ConjunctionOrFilter>();
        if (conjunction.child_filters.empty())
            return false;
        for (const auto& child : conjunction.child_filters)
        {
            Owned<IValueSet> childValues;
            if (!buildTableFilterValues(*child, fieldIndex, record, childValues) || !childValues)
                return false;
            if (values)
                values->unionSet(childValues);
            else
                values.setown(childValues.getClear());
        }
        return true;
    }
    default:
        return false;
    }
}

static bool buildTableFilter(const duckdb::TableFilter& tableFilter, unsigned fieldIndex, const RtlRecord& record, FieldFilterArray& staged)
{
    Owned<IValueSet> values;
    if (!buildTableFilterValues(tableFilter, fieldIndex, record, values))
        return false;
    if (values)
        staged.append(*createFieldFilter(fieldIndex, values));
    return true;
}

// Inexact (string) filters stay in DuckDB's plan, so a repeated filter pushdown pass may offer them again.
static void appendPushedFilter(CScanBindData& bindData, const IFieldFilter& filter)
{
    const std::string filterText = extractFieldFilterText(filter);
    ForEachItemIn(index, bindData.pushedFilters)
    {
        const IFieldFilter& existing = bindData.pushedFilters.item(index);
        if (existing.queryFieldIndex() == filter.queryFieldIndex() && extractFieldFilterText(existing) == filterText)
            return;
    }
    bindData.pushedFilters.append(OLINK(filter));
}

static void pushdownHpccScanFilters(duckdb::ClientContext&, duckdb::LogicalGet& get, duckdb::FunctionData* functionData, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters)
{
    auto& bindData = functionData->Cast<CScanBindData>();
    if (!bindData.expectedRecord)
        return;
    const RtlRecord& record = bindData.expectedRecord->record;
    for (auto iterator = filters.begin(); iterator != filters.end();)
    {
        auto* conjunction = dynamic_cast<duckdb::BoundConjunctionExpression*>(iterator->get());
        if (conjunction && conjunction->type == duckdb::ExpressionType::CONJUNCTION_AND)
        {
            FieldFilterArray staged;
            std::vector<duckdb::unique_ptr<duckdb::Expression>> residualChildren;
            for (auto& child : conjunction->children)
            {
                auto residual = pushdownAndChild(std::move(child), get, record, staged);
                if (residual)
                    residualChildren.push_back(std::move(residual));
            }
            ForEachItemIn(stagedIndex, staged)
                appendPushedFilter(bindData, staged.item(stagedIndex));
            if (residualChildren.empty())
            {
                iterator = filters.erase(iterator);
                continue;
            }
            if (residualChildren.size() == 1)
                *iterator = std::move(residualChildren.front());
            else
            {
                auto residual = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND);
                residual->children = std::move(residualChildren);
                *iterator = duckdb::unique_ptr_cast<duckdb::BoundConjunctionExpression, duckdb::Expression>(std::move(residual));
            }
            ++iterator;
            continue;
        }
        FieldFilterArray staged;
        bool exact = true;
        if (tryBuildScalarFilter(**iterator, get, record, staged, exact))
        {
            appendPushedFilter(bindData, staged.item(0));
            if (exact)
            {
                iterator = filters.erase(iterator);
                continue;
            }
        }
        ++iterator;
    }
}


} // namespace

bool supportsHpccScanPushdownType(const duckdb::FunctionData& functionData, duckdb::idx_t fieldIndex)
{
    return supportsHpccScanPushdownTypeInternal(functionData, fieldIndex);
}

bool buildHpccScanTableFilter(const duckdb::TableFilter& tableFilter, unsigned fieldIndex, const RtlRecord& record, FieldFilterArray& staged)
{
    return buildTableFilter(tableFilter, fieldIndex, record, staged);
}

void mergeHpccScanFieldFilters(FieldFilterArray& filters)
{
    const unsigned numFilters = filters.ordinality();
    std::vector<bool> consumed(numFilters, false);
    FieldFilterArray merged;
    for (unsigned index = 0; index < numFilters; ++index)
    {
        if (consumed[index])
            continue;
        const IFieldFilter& first = filters.item(index);
        Owned<IValueSet> values;
        bool combined = false;
        for (unsigned other = index + 1; other < numFilters; ++other)
        {
            const IFieldFilter& candidate = filters.item(other);
            if (consumed[other] || candidate.queryFieldIndex() != first.queryFieldIndex() || !haveSameFilterType(first, candidate))
                continue;
            if (!values)
            {
                values.setown(extractFieldFilterValues(first));
                if (!values)
                    break;
            }
            Owned<IValueSet> candidateValues = extractFieldFilterValues(candidate);
            if (!candidateValues)
                continue;
            // Scan filters are ANDed, so a row passes all of them exactly when its value is in their intersection.
            values->intersectSet(candidateValues);
            consumed[other] = true;
            combined = true;
        }
        if (combined)
            merged.append(*createFieldFilter(first.queryFieldIndex(), values));
        else
            merged.append(OLINK(first));
    }
    filters.kill();
    ForEachItemIn(mergedIndex, merged)
        filters.append(OLINK(merged.item(mergedIndex)));
}

std::string describeHpccScanFieldFilter(const IFieldFilter& filter, const std::string& fieldName)
{
    return extractFieldFilterDescription(filter, fieldName);
}

std::string joinHpccScanDescriptions(const std::vector<std::string>& descriptions)
{
    return joinDescriptions(descriptions);
}

void configureHpccScanOptimizer(duckdb::ExtensionLoader& loader, duckdb::TableFunction& function)
{
    function.pushdown_complex_filter = pushdownHpccScanFilters;
    function.supports_pushdown_type = supportsHpccScanPushdownTypeInternal;
    function.cardinality = estimateHpccScanCardinality;
    function.to_string = describeHpccScan;

    duckdb::OptimizerExtension optimizer;
    optimizer.optimize_function = optimizeHpccScan;
    duckdb::OptimizerExtension::Register(duckdb::DBConfig::GetConfig(loader.GetDatabaseInstance()), std::move(optimizer));
}

#ifdef _USE_CPPUNIT
class WsSql2OptimizerTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WsSql2OptimizerTests);
    CPPUNIT_TEST(testMergeRangeFilters);
    CPPUNIT_TEST(testMergeDisjointFilters);
    CPPUNIT_TEST(testMergeKeepsUnmergeableFilters);
    CPPUNIT_TEST_SUITE_END();

    RtlIntTypeInfo kindType{ type_unsigned | type_int, 1 };
    RtlStringTypeInfo wordType{ type_string, 4 };
    RtlFieldStrInfo kindField{ "kind", nullptr, &kindType };
    RtlFieldStrInfo wordField{ "word", nullptr, &wordType };
    const RtlFieldInfo* const fields[3] = { &kindField, &wordField, nullptr };
    RtlRecordTypeInfo recordType{ type_record, 5, fields };

    void appendFilters(FieldFilterArray& filters, std::initializer_list<const char*> texts)
    {
        for (const char* text : texts)
            filters.append(*deserializeFieldFilter(0, kindType, text));
    }

    // The merged filters must accept exactly the kind values that every original filter accepts.
    void checkMergedKind(std::initializer_list<const char*> texts)
    {
        FieldFilterArray originals;
        appendFilters(originals, texts);
        FieldFilterArray merged;
        appendFilters(merged, texts);
        mergeHpccScanFieldFilters(merged);
        CPPUNIT_ASSERT_EQUAL(1U, merged.ordinality());
        RtlRecord record(recordType, true);
        for (unsigned value = 0; value < 256; ++value)
        {
            byte row[5] = { static_cast<byte>(value), 'a', 'b', 'c', 'd' };
            RtlDynRow rtlRow(record, row);
            bool expected = true;
            ForEachItemIn(index, originals)
                expected = expected && originals.item(index).matches(rtlRow);
            CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("kind=%u", value).str(), expected, merged.item(0).matches(rtlRow));
        }
    }

    void testMergeRangeFilters()
    {
        checkMergedKind({ "=[2]", "=[1,]" });
        checkMergedKind({ "=[1,10]", "=[5,20]" });
        checkMergedKind({ "=[1,200]", "=(5,7)" });
        checkMergedKind({ "=[1],[3],[5],[7]", "=[3,6]", "=(,5)" });
        checkMergedKind({ "=[2]", "=[2]" });
    }

    void testMergeDisjointFilters()
    {
        checkMergedKind({ "=[1,3]", "=[5,9]" });
        FieldFilterArray filters;
        appendFilters(filters, { "=[1]", "=[2]" });
        mergeHpccScanFieldFilters(filters);
        CPPUNIT_ASSERT_EQUAL(1U, filters.ordinality());
        CPPUNIT_ASSERT(filters.item(0).isEmpty());
    }

    void testMergeKeepsUnmergeableFilters()
    {
        FieldFilterArray filters;
        filters.append(*deserializeFieldFilter(1, wordType, "*"));
        appendFilters(filters, { "=[1,]" });
        filters.append(*deserializeFieldFilter(1, wordType, "=['abcd']"));
        Owned<IValueSet> prefix = createValueSet(wordType);
        deserializeSet(*prefix, "['ab']");
        filters.append(*createSubStringFieldFilter(1, 2, prefix));
        appendFilters(filters, { "=[,3]" });
        mergeHpccScanFieldFilters(filters);
        CPPUNIT_ASSERT_EQUAL(4U, filters.ordinality());
        CPPUNIT_ASSERT(filters.item(0).isWild());
        CPPUNIT_ASSERT_EQUAL(0U, filters.item(1).queryFieldIndex());
        CPPUNIT_ASSERT_EQUAL(std::string("0=[1,3]"), extractFieldFilterText(filters.item(1)));
        CPPUNIT_ASSERT_EQUAL(1U, filters.item(2).queryFieldIndex());
        CPPUNIT_ASSERT_EQUAL(1U, filters.item(3).queryFieldIndex());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(WsSql2OptimizerTests);
#endif // _USE_CPPUNIT

} // namespace duckdb::hpcc
