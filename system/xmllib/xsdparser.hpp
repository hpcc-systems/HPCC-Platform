/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef __SCHEMAPARSER_HPP__
#define __SCHEMAPARSER_HPP__

#include <vector>
#include <string>
#include "jiface.hpp"
#include "jptree.hpp"
#include "xmllib.hpp"

typedef std::vector<std::string> StringStack;

// For array:
//  - isArray() returns true, 
//  - isComplexType() returns false
//  - getFieldCount() returns 1
//  - getFieldType(0) returns the item type
//  - getFieldName(0) returns the item name
// For simple type:
//  - getFieldCount() returns 0, and queryFieldType() and queryFieldName() shouldn't be called
// 

interface XMLLIB_API IXmlAttribute : implements IInterface
{
    virtual const char* queryName() = 0;
    virtual const char* queryTypeName() = 0;

    virtual const char* getDefaultValue() = 0;
    virtual bool  isUseRequired() = 0;
    virtual const char* getFixedValue() = 0;

    virtual const char* getSampleValue(StringBuffer& out) = 0;
};

enum XmlSubType
{
    // Default
    SubType_Default,

    // Array
    SubType_Array,

    // SimpleType
    SubType_Simple_Restriction,
    SubType_Simple_Enumeration,

    // Complex Type
    SubType_Complex_Sequence, 
    SubType_Complex_All, 
    SubType_Complex_Choice, 
    SubType_Complex_SimpleContent
};

interface XMLLIB_API IXmlType : implements IInterface
{
    virtual const char* queryName() = 0;

    virtual bool isComplexType() = 0;
    virtual size_t  getFieldCount() = 0;
    virtual IXmlType* queryFieldType(int idx) = 0;
    virtual const char* queryFieldName(int idx) = 0; 
    virtual bool queryFieldRepeats(int idx) = 0;

    virtual bool isArray() = 0; 
    virtual XmlSubType getSubType() = 0;

    virtual bool hasDefaultValue() = 0;
    virtual const char* getDefaultValue() = 0;

    virtual size_t getAttrCount() = 0;
    virtual IXmlAttribute* queryAttr(int idx) = 0;
    virtual const char* queryAttrName(int idx) = 0; 

    // only defined for simple type
    virtual void getSampleValue(StringBuffer& out, const char* fieldName) = 0; 

    // for debug
    virtual void toString(class StringBuffer& s, int indent, StringStack& parent) = 0;
};

interface XMLLIB_API IXmlSchema : implements IInterface
{
    virtual IXmlType* queryElementType(const char* name) = 0;
    virtual IXmlType* queryTypeByName(const char* name) = 0;
};

extern "C" XMLLIB_API IXmlSchema* createXmlSchemaFromFile(const char* file); 
extern "C" XMLLIB_API IXmlSchema* createXmlSchemaFromString(const char* src); 
extern "C" XMLLIB_API IXmlSchema* createXmlSchemaFromPTree(IPTree* schema); 

#endif
