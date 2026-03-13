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

#pragma once

#include "jlib.hpp"
#include "jstring.hpp"
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <string>

//==============================================================================
// Base Type System
//==============================================================================

enum class BaseType
{
    Boolean,
    NullValue,
    PosNumber,
    NegNumber,
    Float,
    DefaultString,
    String
};

//==============================================================================
// Command-Line Arguments Structure
//==============================================================================

struct ParsedArgs
{
    std::vector<std::string> files;
    std::string stringType;
    bool fullScan;
    bool readFromStdin;
    
    ParsedArgs() 
        : stringType("UTF8"), fullScan(false), readFromStdin(false) 
    {}
};

//==============================================================================
// Type Set - Collection of types for handling mixed-type fields
//==============================================================================

class TypeSet
{
private:
    std::set<BaseType> types;

public:
    bool addType(BaseType type);  // Returns true if type was newly added
    BaseType reduce() const;
    bool isEmpty() const;
    size_t count() const;
    bool contains(BaseType type) const;
    const std::set<BaseType>& getTypes() const;
    std::vector<std::string> getTypeDescriptions() const;
};

//==============================================================================
// Schema Items - Represent structure of JSON/XML data
//==============================================================================

class SchemaItem
{
public:
    virtual ~SchemaItem() = default;
};

// Simple value with type tracking
class ValueItem : public SchemaItem
{
private:
    TypeSet types;

public:
    bool addValueType(BaseType type);  // Returns true if type was newly added
    const TypeSet& getTypes() const;
};

// Represents JSON/XML objects with named fields
class ObjectItem : public SchemaItem
{
private:
    // Using vector to preserve insertion order
    std::vector<std::pair<std::string, std::unique_ptr<SchemaItem>>> fields;

public:
    bool addOrMergeField(const std::string& name, std::unique_ptr<SchemaItem> item);  // Returns true if schema changed
    const std::vector<std::pair<std::string, std::unique_ptr<SchemaItem>>>& getFields() const;
};

// Represents JSON/XML arrays
class ArrayItem : public SchemaItem
{
private:
    std::unique_ptr<SchemaItem> elementType;

public:
    void setElementType(std::unique_ptr<SchemaItem> item);
    bool mergeElementType(std::unique_ptr<SchemaItem> item);  // Returns true if schema changed
    const SchemaItem* getElementType() const;
};

//==============================================================================
// Utility Functions
//==============================================================================

// ECL keyword checking
bool isEclKeyword(const std::string& name);

// Name sanitization
std::string removeIllegalChars(const std::string& name, 
                               char replacementChar = '_',
                               const std::set<char>& keepChars = {});

// Name formatting
std::string applyPrefix(const std::string& name, const std::string& prefix);
std::string legalLayoutSubname(const std::string& name);
std::string asLayoutName(const std::string& name, std::set<std::string>& usedNames);
std::string asEclFieldName(const std::string& name);
std::string asEclXpath(const std::string& name);
std::string asDatasetType(const std::string& name);

// Type conversion
BaseType stringValueToBaseType(const char* value);
std::string asEclType(const TypeSet& types, const std::string& stringType);
std::string asValueComment(const TypeSet& types);

// Type merging - implements the algorithm from Lisp code
BaseType commonType(BaseType newType, BaseType oldType);
BaseType reduceBaseType(const TypeSet& types);

// Schema item cloning
std::unique_ptr<SchemaItem> cloneSchemaItem(const SchemaItem* item);

// Schema item merging
bool mergeSchemaItems(SchemaItem* target, const SchemaItem* source);

// ECL Generation
std::string generateEclFieldDef(const SchemaItem* item, 
                                const std::string& name,
                                std::set<std::string>& usedLayoutNames,
                                const std::string& stringType);
std::string generateEclRecordDef(const SchemaItem* item, 
                                 const std::string& name,
                                 std::set<std::string>& usedLayoutNames,
                                 const std::string& stringType);
