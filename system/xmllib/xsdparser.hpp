/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
