/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "common.hpp"
#include "reservedwords.hpp"
#include <algorithm>
#include <cctype>
#include <numeric>
#include <sstream>

//==============================================================================
// TypeSet Implementation
//==============================================================================

bool TypeSet::addType(BaseType type)
{
    auto result = types.insert(type);
    return result.second;  // true if newly inserted
}

BaseType TypeSet::reduce() const
{
    if (types.empty())
        return BaseType::DefaultString;
    
    if (types.size() == 1)
        return *types.begin();
    
    // Use std::accumulate to apply commonType repeatedly
    return std::accumulate(
        std::next(types.begin()),
        types.end(),
        *types.begin(),
        commonType
    );
}

bool TypeSet::isEmpty() const
{
    return types.empty();
}

size_t TypeSet::count() const
{
    return types.size();
}

bool TypeSet::contains(BaseType type) const
{
    return types.find(type) != types.end();
}

const std::set<BaseType>& TypeSet::getTypes() const
{
    return types;
}

std::vector<std::string> TypeSet::getTypeDescriptions() const
{
    std::vector<std::string> descriptions;
    for (BaseType type : types)
    {
        switch (type)
        {
            case BaseType::Boolean:
                descriptions.push_back("boolean");
                break;
            case BaseType::NullValue:
                descriptions.push_back("nullable");
                break;
            case BaseType::PosNumber:
                descriptions.push_back("unsigned integer");
                break;
            case BaseType::NegNumber:
                descriptions.push_back("signed integer");
                break;
            case BaseType::Float:
                descriptions.push_back("float");
                break;
            case BaseType::DefaultString:
                descriptions.push_back("string");
                break;
            case BaseType::String:
                descriptions.push_back("string");
                break;
        }
    }
    return descriptions;
}

//==============================================================================
// ValueItem Implementation
//==============================================================================

bool ValueItem::addValueType(BaseType type)
{
    return types.addType(type);  // Returns true if type was newly added
}

const TypeSet& ValueItem::getTypes() const
{
    return types;
}

//==============================================================================
// ObjectItem Implementation
//==============================================================================

bool ObjectItem::addOrMergeField(const std::string& name, std::unique_ptr<SchemaItem> item)
{
    // Look for existing field with this name
    for (auto& fieldPair : fields)
    {
        if (fieldPair.first == name)
        {
            // Field exists - merge the items
            SchemaItem* existing = fieldPair.second.get();
            
            // If both are ValueItems, merge their types
            if (ValueItem* existingValue = dynamic_cast<ValueItem*>(existing))
            {
                if (ValueItem* newValue = dynamic_cast<ValueItem*>(item.get()))
                {
                    // Merge all types from newValue into existingValue
                    bool changed = false;
                    for (BaseType type : newValue->getTypes().getTypes())
                    {
                        if (existingValue->addValueType(type))
                            changed = true;
                    }
                    return changed;
                }
            }
            
            // If both are ObjectItems, recursively merge their fields
            if (ObjectItem* existingObject = dynamic_cast<ObjectItem*>(existing))
            {
                if (ObjectItem* newObject = dynamic_cast<ObjectItem*>(item.get()))
                {
                    // Recursively merge all fields from newObject into existingObject
                    bool changed = false;
                    const auto& newFields = newObject->getFields();
                    for (const auto& newFieldPair : newFields)
                    {
                        auto clonedItem = cloneSchemaItem(newFieldPair.second.get());
                        if (existingObject->addOrMergeField(newFieldPair.first, std::move(clonedItem)))
                            changed = true;
                    }
                    return changed;
                }
            }
           
            // If both are ArrayItems, merge their element types
            if (ArrayItem* existingArray = dynamic_cast<ArrayItem*>(existing))
            {
                if (ArrayItem* newArray = dynamic_cast<ArrayItem*>(item.get()))
                {
                    if (newArray->getElementType())
                    {
                        auto clonedElem = cloneSchemaItem(newArray->getElementType());
                        return existingArray->mergeElementType(std::move(clonedElem));
                    }
                    return false;
                }
                // If existing is ArrayItem but new is ObjectItem, merge the ObjectItem into array's elementType
                else if (dynamic_cast<ObjectItem*>(item.get()))
                {
                    return existingArray->mergeElementType(std::move(item));
                }
            }
            
            // If existing is ObjectItem but new is ArrayItem, convert to ArrayItem
            if (ObjectItem* existingObject = dynamic_cast<ObjectItem*>(existing))
            {
                if (ArrayItem* newArray = dynamic_cast<ArrayItem*>(item.get()))
                {
                    // Convert existing ObjectItem to ArrayItem with existingObject as element type
                    auto arrayItem = std::make_unique<ArrayItem>();
                    // Clone the existing object as the first element
                    auto clonedExisting = cloneSchemaItem(existingObject);
                    arrayItem->mergeElementType(std::move(clonedExisting));
                    // Merge the new array's element type
                    if (newArray->getElementType())
                    {
                        auto clonedElem = cloneSchemaItem(newArray->getElementType());
                        arrayItem->mergeElementType(std::move(clonedElem));
                    }
                    // Replace existing with the new array
                    fieldPair.second = std::move(arrayItem);
                    return true;  // Schema changed (ObjectItem -> ArrayItem)
                }
            }
            
            // If types don't match, replace with the more specific type
            // An ObjectItem or ArrayItem is more specific than a ValueItem (which might be from null)
            if (dynamic_cast<ValueItem*>(existing))
            {
                // Existing is a ValueItem - replace it with the new item if it's more specific
                if (dynamic_cast<ObjectItem*>(item.get()) || dynamic_cast<ArrayItem*>(item.get()))
                {
                    fieldPair.second = std::move(item);
                    return true;  // Schema changed (ValueItem -> Object/Array)
                }
            }
            else if (dynamic_cast<ObjectItem*>(existing) || dynamic_cast<ArrayItem*>(existing))
            {
                // Existing is ObjectItem or ArrayItem - keep it (more specific than ValueItem)
                if (dynamic_cast<ValueItem*>(item.get()))
                {
                    return false;  // No change
                }
            }
            
            // If neither is more specific, we have a real schema conflict
            // For now, keep the existing one
            return false;
        }
    }
    
    // Field doesn't exist - add it
    fields.push_back(std::make_pair(name, std::move(item)));
    return true;  // Schema changed (new field added)
}

const std::vector<std::pair<std::string, std::unique_ptr<SchemaItem>>>& ObjectItem::getFields() const
{
    return fields;
}

//==============================================================================
// ArrayItem Implementation
//==============================================================================

void ArrayItem::setElementType(std::unique_ptr<SchemaItem> item)
{
    elementType = std::move(item);
}

bool ArrayItem::mergeElementType(std::unique_ptr<SchemaItem> item)
{
    if (!elementType)
    {
        // No existing element type - just set it
        elementType = std::move(item);
        return true;  // Schema changed (new element type added)
    }
    
    // Merge with existing element type
    SchemaItem* existing = elementType.get();
    
    // If both are ValueItems, merge their types
    if (ValueItem* existingValue = dynamic_cast<ValueItem*>(existing))
    {
        if (ValueItem* newValue = dynamic_cast<ValueItem*>(item.get()))
        {
            // Merge all types from newValue into existingValue
            bool changed = false;
            for (BaseType type : newValue->getTypes().getTypes())
            {
                if (existingValue->addValueType(type))
                    changed = true;
            }
            return changed;
        }
    }
    
    // If both are ObjectItems, recursively merge
    if (ObjectItem* existingObject = dynamic_cast<ObjectItem*>(existing))
    {
        if (ObjectItem* newObject = dynamic_cast<ObjectItem*>(item.get()))
        {
            // Recursively merge all fields
            bool changed = false;
            const auto& newFields = newObject->getFields();
            for (const auto& newFieldPair : newFields)
            {
                auto clonedItem = cloneSchemaItem(newFieldPair.second.get());
                if (existingObject->addOrMergeField(newFieldPair.first, std::move(clonedItem)))
                    changed = true;
            }
            return changed;
        }
    }
    
    // If both are ArrayItems, recursively merge their element types  
    if (ArrayItem* existingArray = dynamic_cast<ArrayItem*>(existing))
    {
        if (ArrayItem* newArray = dynamic_cast<ArrayItem*>(item.get()))
        {
            if (newArray->getElementType())
            {
                auto clonedElem = cloneSchemaItem(newArray->getElementType());
                return existingArray->mergeElementType(std::move(clonedElem));
            }
            return false;
        }
    }
    
    // Type mismatch - keep existing
    return false;
}

const SchemaItem* ArrayItem::getElementType() const
{
    return elementType.get();
}

//==============================================================================
// Utility Functions
//==============================================================================

bool isEclKeyword(const std::string& name)
{
    // searchReservedWords() is case-sensitive and expects lowercase
    std::string lowerName = name;
    std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return searchReservedWords(lowerName.c_str());
}

std::string removeIllegalChars(const std::string& name, 
                               char replacementChar,
                               const std::set<char>& keepChars)
{
    std::string result;
    result.reserve(name.length());
    
    char lastChar = '\0';
    for (char c : name)
    {
        bool isLegal = std::isalnum(static_cast<unsigned char>(c)) || 
                      keepChars.find(c) != keepChars.end();
        
        if (isLegal)
        {
            result += c;
            lastChar = c;
        }
        else if (lastChar != replacementChar)
        {
            result += replacementChar;
            lastChar = replacementChar;
        }
    }
    
    return result;
}

std::string applyPrefix(const std::string& name, const std::string& prefix)
{
    if (name.empty())
        return prefix;
    return prefix + name;
}

std::string legalLayoutSubname(const std::string& name)
{
    if (name.empty())
        return "UNNAMED";
    
    // Remove illegal characters, keeping underscores
    std::set<char> keepChars = {'_'};
    std::string cleaned = removeIllegalChars(name, '_', keepChars);
    
    // Ensure starts with letter or underscore
    if (!cleaned.empty() && !std::isalpha(static_cast<unsigned char>(cleaned[0])) && cleaned[0] != '_')
        cleaned = "f_" + cleaned;
    
    // Check if it's an ECL keyword and prefix if needed
    if (isEclKeyword(cleaned))
        cleaned = "f_" + cleaned;
    
    return cleaned;
}

std::string asLayoutName(const std::string& name, std::set<std::string>& usedNames)
{
    std::string baseName = legalLayoutSubname(name);
    
    // Convert to uppercase for layout names
    std::transform(baseName.begin(), baseName.end(), baseName.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    
    // Check if this exact base name is already registered
    std::string layoutName = baseName + "_LAYOUT";
    
    // If already in set, return it (same field referenced multiple times)
    if (usedNames.find(layoutName) != usedNames.end())
        return layoutName;
    
    // Not in set, need to add it
    usedNames.insert(layoutName);
    return layoutName;
}

std::string asEclFieldName(const std::string& name)
{
    if (name.empty())
        return "unnamed";
    
    // Handle XML attribute names (start with @)
    std::string workingName = name;
    if (!workingName.empty() && workingName[0] == '@')
        workingName = workingName.substr(1); // Strip @ prefix for field name
    
    // Remove illegal characters, keeping underscores
    std::set<char> keepChars = {'_'};
    std::string cleaned = removeIllegalChars(workingName, '_', keepChars);
    
    // Convert dashes to underscores is already handled by removeIllegalChars
    
    // Ensure starts with letter or underscore (but not double underscore)
    if (!cleaned.empty())
    {
        // Check for double underscore prefix (reserved in C++/ECL)
        if (cleaned.size() >= 2 && cleaned[0] == '_' && cleaned[1] == '_')
        {
            // Replace __ prefix with f_
            cleaned = "f_" + cleaned.substr(2);
        }
        else if (!std::isalpha(static_cast<unsigned char>(cleaned[0])) && cleaned[0] != '_')
        {
            // Doesn't start with letter or underscore
            cleaned = "f_" + cleaned;
        }
    }
    
    // Check if it's an ECL keyword and prefix if needed
    if (isEclKeyword(cleaned))
        cleaned = "f_" + cleaned;
    
    return cleaned;
}

std::string asEclXpath(const std::string& name)
{
    // Handle XML inner text value - use empty XPATH
    if (name == "_inner_value")
        return "{XPATH('')}";
    
    // For other fields (including attributes with @ prefix), use name as-is
    return "{XPATH('" + name + "')}";
}

std::string asDatasetType(const std::string& layoutName)
{
    return "DATASET(" + layoutName + ")";
}

std::string asEclType(const TypeSet& types, const std::string& stringType)
{
    BaseType reducedType = types.reduce();
    
    switch (reducedType)
    {
        case BaseType::Boolean:
            return "BOOLEAN";
        case BaseType::NullValue:
            return stringType;
        case BaseType::PosNumber:
            return "UNSIGNED";
        case BaseType::NegNumber:
            return "INTEGER";
        case BaseType::Float:
            return "REAL";
        case BaseType::DefaultString:
            return stringType;
        case BaseType::String:
            return "STRING";
        default:
            return stringType;
    }
}

std::string asValueComment(const TypeSet& types)
{
    // Only generate comment if there are multiple types or special cases
    if (types.count() <= 1)
    {
        // Special case: single null-value type
        if (types.count() == 1 && types.contains(BaseType::NullValue))
            return " // nullable";
        return "";
    }
    
    // Get reduced type and see if it's a string-based type
    BaseType reducedType = types.reduce();
    bool isStringResult = (reducedType == BaseType::DefaultString || 
                          reducedType == BaseType::String);
    
    if (!isStringResult)
        return "";
    
    // Generate comment listing the original types
    std::vector<std::string> descriptions = types.getTypeDescriptions();
    if (descriptions.empty())
        return "";
    
    std::string comment = " // ";
    for (size_t i = 0; i < descriptions.size(); ++i)
    {
        if (i > 0)
            comment += ", ";
        comment += descriptions[i];
    }
    
    return comment;
}

BaseType stringValueToBaseType(const char* value)
{
    if (!value || !*value)
        return BaseType::NullValue;
    
    // Try to determine type from value
    if (stricmp(value, "true") == 0 || stricmp(value, "false") == 0)
        return BaseType::Boolean;
    
    // Try to parse as number
    const char* p = value;
    bool isNegative = false;
    bool hasDecimal = false;
    
    // Skip leading whitespace
    while (*p && std::isspace(static_cast<unsigned char>(*p))) p++;
    
    // Check for sign
    if (*p == '-')
    {
        isNegative = true;
        p++;
    }
    else if (*p == '+')
        p++;
    
    // Check for digits
    bool hasDigits = false;
    while (*p && std::isdigit(static_cast<unsigned char>(*p)))
    {
        hasDigits = true;
        p++;
    }
    
    // Check for decimal point
    if (*p == '.')
    {
        hasDecimal = true;
        p++;
        while (*p && std::isdigit(static_cast<unsigned char>(*p)))
            p++;
    }
    
    // Check for exponent
    if (hasDigits && (*p == 'e' || *p == 'E'))
    {
        hasDecimal = true; // Treat exponential notation as float
        p++;
        if (*p == '+' || *p == '-')
            p++;
        while (*p && std::isdigit(static_cast<unsigned char>(*p)))
            p++;
    }
    
    // Skip trailing whitespace
    while (*p && std::isspace(static_cast<unsigned char>(*p))) p++;
    
    // If we parsed a valid number
    if (hasDigits && *p == '\0')
    {
        if (hasDecimal)
            return BaseType::Float;
        else
            return isNegative ? BaseType::NegNumber : BaseType::PosNumber;
    }
    
    // Otherwise it's a string
    return BaseType::DefaultString;
}

BaseType commonType(BaseType newType, BaseType oldType)
{
    // Rule 1: If either type is considered "missing" (this shouldn't happen in our implementation)
    // but we handle it for completeness
    
    // Rule 2: If both types are identical, return that type
    if (newType == oldType)
        return newType;
    
    // Rule 3: If either type is default-string, return default-string
    if (newType == BaseType::DefaultString || oldType == BaseType::DefaultString)
        return BaseType::DefaultString;
    
    // Rule 4: If either type is string, return string
    if (newType == BaseType::String || oldType == BaseType::String)
        return BaseType::String;
    
    // Rule 5: If one is pos-number and the other is neg-number, return neg-number
    if ((newType == BaseType::PosNumber && oldType == BaseType::NegNumber) ||
        (newType == BaseType::NegNumber && oldType == BaseType::PosNumber))
        return BaseType::NegNumber;
    
    // Rule 6: If any integer type combined with float, return float
    bool newIsInt = (newType == BaseType::PosNumber || newType == BaseType::NegNumber);
    bool oldIsInt = (oldType == BaseType::PosNumber || oldType == BaseType::NegNumber);
    if ((newIsInt && oldType == BaseType::Float) || (oldIsInt && newType == BaseType::Float))
        return BaseType::Float;
    
    // Rule 7: Otherwise (incompatible types), return string
    return BaseType::String;
}

BaseType reduceBaseType(const TypeSet& types)
{
    return types.reduce();
}

std::unique_ptr<SchemaItem> cloneSchemaItem(const SchemaItem* item)
{
    if (!item)
        return nullptr;
    
    // Clone ValueItem
    if (const ValueItem* vi = dynamic_cast<const ValueItem*>(item))
    {
        auto cloned = std::make_unique<ValueItem>();
        for (BaseType type : vi->getTypes().getTypes())
            cloned->addValueType(type);
        return cloned;
    }
    
    // Clone ObjectItem
    if (const ObjectItem* oi = dynamic_cast<const ObjectItem*>(item))
    {
        auto cloned = std::make_unique<ObjectItem>();
        for (const auto& field : oi->getFields())
        {
            auto clonedField = cloneSchemaItem(field.second.get());
            cloned->addOrMergeField(field.first, std::move(clonedField));
        }
        return cloned;
    }
    
    // Clone ArrayItem
    if (const ArrayItem* ai = dynamic_cast<const ArrayItem*>(item))
    {
        auto cloned = std::make_unique<ArrayItem>();
        if (ai->getElementType())
        {
            auto clonedElem = cloneSchemaItem(ai->getElementType());
            cloned->setElementType(std::move(clonedElem));
        }
        return cloned;
    }
    
    return nullptr;
}

bool mergeSchemaItems(SchemaItem* target, const SchemaItem* source)
{
    if (!target || !source)
        return false;
    
    // If target is ValueItem
    if (ValueItem* targetValue = dynamic_cast<ValueItem*>(target))
    {
        if (const ValueItem* sourceValue = dynamic_cast<const ValueItem*>(source))
        {
            // Merge types from source into target
            bool changed = false;
            for (BaseType type : sourceValue->getTypes().getTypes())
            {
                if (targetValue->addValueType(type))
                    changed = true;
            }
            return changed;
        }
        // Note: Can't merge ObjectItem/ArrayItem into ValueItem
        return false;
    }
    
    // If target is ObjectItem
    if (ObjectItem* targetObject = dynamic_cast<ObjectItem*>(target))
    {
        if (const ObjectItem* sourceObject = dynamic_cast<const ObjectItem*>(source))
        {
            // Merge all fields from source into target
            bool changed = false;
            const auto& sourceFields = sourceObject->getFields();
            for (const auto& fieldPair : sourceFields)
            {
                auto clonedItem = cloneSchemaItem(fieldPair.second.get());
                if (targetObject->addOrMergeField(fieldPair.first, std::move(clonedItem)))
                    changed = true;
            }
            return changed;
        }
        return false;
    }
    
    // If target is ArrayItem
    if (ArrayItem* targetArray = dynamic_cast<ArrayItem*>(target))
    {
        if (const ArrayItem* sourceArray = dynamic_cast<const ArrayItem*>(source))
        {
            if (sourceArray->getElementType())
            {
                auto clonedElem = cloneSchemaItem(sourceArray->getElementType());
                return targetArray->mergeElementType(std::move(clonedElem));
            }
        }
        return false;
    }
    
    return false;
}

std::string generateEclFieldDef(const SchemaItem* item, 
                                const std::string& name,
                                std::set<std::string>& usedLayoutNames,
                                const std::string& stringType)
{
    if (!item)
        return "";
    
    std::string eclFieldName = asEclFieldName(name);
    std::string xpath = asEclXpath(name);
    
    // Check if it's a ValueItem (simple type)
    if (const ValueItem* valueItem = dynamic_cast<const ValueItem*>(item))
    {
        const TypeSet& types = valueItem->getTypes();
        std::string eclType = asEclType(types, stringType);
        std::string comment = asValueComment(types);
        
        std::ostringstream out;
        out << "    " << eclType << " " << eclFieldName << " " << xpath << ";";
        if (!comment.empty())
            out << comment;
        out << "\n";
        return out.str();
    }
    
    // Check if it's an ObjectItem (nested object)
    if (dynamic_cast<const ObjectItem*>(item))
    {
        std::string layoutName = asLayoutName(name, usedLayoutNames);
        std::string datasetType = asDatasetType(layoutName);
        
        std::ostringstream out;
        out << "    " << datasetType << " " << eclFieldName << " " << xpath << ";\n";
        return out.str();
    }
    
    // Check if it's an ArrayItem
    if (const ArrayItem* arrayItem = dynamic_cast<const ArrayItem*>(item))
    {
        const SchemaItem* elementType = arrayItem->getElementType();
        
        std::ostringstream out;
        out << "    ";
        
        if (!elementType)
        {
            out << "SET OF " << stringType << " ";
        }
        else if (const ValueItem* valueElement = dynamic_cast<const ValueItem*>(elementType))
        {
            // Array of simple values - use SET OF
            const TypeSet& types = valueElement->getTypes();
            std::string eclType = asEclType(types, stringType);
            out << "SET OF " << eclType << " ";
        }
        else
        {
            // Array of complex objects - use DATASET
            std::string layoutName = asLayoutName(name, usedLayoutNames);
            std::string datasetType = asDatasetType(layoutName);
            out << datasetType << " ";
        }
        
        out << eclFieldName << " " << xpath << ";\n";
        return out.str();
    }
    
    return "";
}

std::string generateEclRecordDef(const SchemaItem* item, 
                                 const std::string& name,
                                 std::set<std::string>& usedLayoutNames,
                                 const std::string& stringType)
{
    if (!item)
        return "";
    
    // Simple values don't generate record definitions
    if (dynamic_cast<const ValueItem*>(item))
        return "";
    
    std::ostringstream result;
    
    // Check if it's an ObjectItem
    if (const ObjectItem* objectItem = dynamic_cast<const ObjectItem*>(item))
    {
        std::string layoutName = asLayoutName(name, usedLayoutNames);
        
        // First, generate child record definitions (depth-first)
        const auto& fields = objectItem->getFields();
        for (const auto& fieldPair : fields)
        {
            const std::string& fieldName = fieldPair.first;
            const SchemaItem* fieldItem = fieldPair.second.get();
            
            std::string childRecDef = generateEclRecordDef(fieldItem, fieldName, usedLayoutNames, stringType);
            if (!childRecDef.empty())
                result << childRecDef;
        }
        
        // Now generate this record definition
        result << layoutName << " := RECORD\n";
        
        for (const auto& fieldPair : fields)
        {
            const std::string& fieldName = fieldPair.first;
            const SchemaItem* fieldItem = fieldPair.second.get();
            
            result << generateEclFieldDef(fieldItem, fieldName, usedLayoutNames, stringType);
        }
        
        result << "END;\n\n";
        
        return result.str();
    }
    
    // Check if it's an ArrayItem
    if (const ArrayItem* arrayItem = dynamic_cast<const ArrayItem*>(item))
    {
        const SchemaItem* elementType = arrayItem->getElementType();
        if (!elementType)
            return "";
        
        // Only generate record def for complex element types
        if (dynamic_cast<const ValueItem*>(elementType))
            return "";  // Simple array elements don't need record definitions
        
        return generateEclRecordDef(elementType, name, usedLayoutNames, stringType);
    }
    
    return "";
}
