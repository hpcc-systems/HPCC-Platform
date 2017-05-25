/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#ifndef _SCHEMA_COMMON_HPP_
#define _SCHEMA_COMMON_HPP_

#include <iostream>
#include "jiface.hpp"
#include "jstring.hpp"
#include "jlib.hpp"
#include "jlog.hpp"
#include "jarray.hpp"
#include "XMLTags.h"
#include "ExceptionStrings.hpp"
#include "build-config.h"

namespace CONFIGURATOR
{

#define MINIMUM_STRICTNESS  0
#define DEFAULT_STRICTNESS  5
#define MAXIMUM_STRICTNESS  10
#define STRICTNESS_LEVEL MINIMUM_STRICTNESS

#define QUICK_OUT(X,Y,Z) quickOut(X,#Y,get##Y(),Z);
#define QUICK_OUT_2(Y) quickOut(cout, #Y, get##Y(), offset);
#define QUICK_OUT_3(X) if (m_p##X != nullptr) m_p##X->dump(cout, offset);
#define QUICK_OUT_ARRAY(X,Z) for (int idx=0; idx < this->length(); idx++)               \
                             {                                                          \
                                quickOutPad(X,Z+STANDARD_OFFSET_1);                     \
                                X << idx+1 << "]" << ::std::endl;                         \
                                (this->item(idx)).dump(cout,Z);                         \
                             }

#define QUICK_DOC_ARRAY(X) for (int idx=0; idx < this->length(); idx++)                 \
                           {                                                            \
                                (this->item(idx)).getDocumentation(X);                  \
                           }
#define LAST_ONLY -1
#define LAST_AND_FIRST  -2

#define QUICK_ENV_XPATH(X) for (int idx=0; idx < this->length(); idx++)                 \
                            {                                                           \
                                 (this->item(idx)).populateEnvXPath(X.str(), idx+1);    \
                            }
#define QUICK_ENV_XPATH_WITH_INDEX(X,Y) for (int idx=0; idx < this->length(); idx++)    \
                            {                                                           \
                                 (this->item(idx)).populateEnvXPath(X.str(), Y);        \
                            }

#define QUICK_LOAD_ENV_XML(X)   assert(X != nullptr);                                      \
                                for (int idx=0; idx < this->length(); idx++)            \
                                {                                                       \
                                     (this->item(idx)).loadXMLFromEnvXml(X);            \
                                }

#define GETTER(X) virtual const char* get##X() const { return m_str##X.str(); }
#define SETTER(X) virtual void set##X(const char* p) { m_str##X.clear().append(p); }
#define GETTERSETTER(X) protected: StringBuffer m_str##X; public: GETTER(X) SETTER(X) public:

//#define GETTER2(X) virtual const char* get##X() const { return m_str##X.str(); }
//#define SETTER2(X) virtual void set##X(const char* p) { m_str##X.clear().append(p); m_str##X.replace('/','_');}
//#define GETTERSETTER2(X) protected: StringBuffer m_str##X; public: GETTER(X) SETTER2(X) public:

#define GETTERINT(X) virtual const long get##X() const { return m_n##X; }
#define SETTERINT(X) virtual void set##X(long p) { m_n##X = p; } virtual void set##X(const char *p) { assert(p != nullptr); if (p != 0 && *p != 0) m_n##X = atol(p); }
#define GETTERSETTERINT(X) protected: long m_n##X; public: GETTERINT(X) SETTERINT(X) private:

#define SETPARENTNODE(X, Y) if (X!= nullptr && Y != nullptr) X->setParentNode(Y);
//#define DEBUG_MARK_STRDOC strDoc.append(__FILE__).append(":").append(__LINE__).append("\n");
#define DEBUG_MARK_STRDOC
#define DEBUG_MARK_COMMENT(X) X.append("//  ").append(__FILE__).append(":").append(__LINE__).append("\n");
#define DEBUG_MARK_COMMENT2(X,Y) X.append("//  UIType=").append(Y->getUIType()).append("  ").append(__FILE__).append(":").append(__LINE__).append("\n");
#define DEBUG_MARK_COMMENT_3 quickOutPad(strJSON, offset); strJSON.append("{ \"").append(__FILE__).append("\" : \"").append(__LINE__).append("\"},\n"); QuickOutPad(strJSON, offset);
#define DEBUG_MARK_COMMENT_4 quickOutPad(strJSON, offset); strJSON.append(",{ \"").append(__FILE__).append("\" : \"").append(__LINE__).append("\"},\n"); QuickOutPad(strJSON, offset);
#define DEBUG_MARK_STRJS DEBUG_MARK_COMMENT(strJS)
#define DEBUG_MARK_JSON_1 //DEBUG_MARK_COMMENT_4// DEBUG_MARK_COMMENT(strJSON)
#define DEBUG_MARK_JSON_2 //DEBUG_MARK_COMMENT_3// DEBUG_MARK_COMMENT(strJSON)

#define GETTERTYPE(X) C##X* get##X() const { return m_p##X; }
#define SETTERTYPE(X) void set##X( C##X *p ) { assert(p != nullptr); if (p != nullptr) m_p##X = p; }
#define GETTERSETTERTYPE(X) public: C##X *m_p##X; GETTERTYPE(X) SETTERTYPE(X) private:

#define CHECK_EXCLUSION(X)  if(CConfigSchemaHelper::getInstance()->getSchemaMapManager()->isNodeExcluded(X)) return nullptr;
#define IS_EXCLUDED(X)      (CConfigSchemaHelper::getInstance()->getSchemaMapManager()->isNodeExcluded(X) ? true : false)

enum NODE_TYPES
{
    XSD_ANNOTATION = 0x0,
    XSD_APP_INFO,
    XSD_ATTRIBUTE,
    XSD_ATTRIBUTE_ARRAY,
    XSD_ATTRIBUTE_GROUP,
    XSD_ATTRIBUTE_GROUP_ARRAY,
    XSD_CHOICE,
    XSD_COMPLEX_CONTENT,
    XSD_COMPLEX_TYPE,
    XSD_COMPLEX_TYPE_ARRAY,
    XSD_DOCUMENTATION,
    XSD_ELEMENT,
    XSD_ELEMENT_ARRAY,
    XSD_ARRAY_OF_ELEMENT_ARRAYS,
    XSD_EXTENSION,
    XSD_FIELD,
    XSD_FIELD_ARRAY,
    XSD_KEY,
    XSD_KEY_ARRAY,
    XSD_KEYREF,
    XSD_KEYREF_ARRAY,
    XSD_INCLUDE,
    XSD_INCLUDE_ARRAY,
    XSD_RESTRICTION,
    XSD_SCHEMA,
    XSD_SEQUENCE,
    XSD_SIMPLE_TYPE,
    XSD_SIMPLE_TYPE_ARRAY,
    XSD_ENUMERATION,
    XSD_ENUMERATION_ARRAY,
    XSD_LENGTH,
    XSD_FRACTION_DIGITS,
    XSD_MAX_EXCLUSIVE,
    XSD_MAX_INCLUSIVE,
    XSD_MIN_EXCLUSIVE,
    XSD_MIN_INCLUSIVE,
    XSD_MIN_LENGTH,
    XSD_MAX_LENGTH,
    XSD_PATTERN,
    XSD_SELECTOR,
    XSD_SIMPLE_CONTENT,
    XSD_TOTAL_DIGITS,
    XSD_UNIQUE,
    XSD_UNIQUE_ARRAY,
    XSD_WHITE_SPACE,
    XSD_DT_NORMALIZED_STRING,  // keep this as the first DT type for array index purposes
    XSD_DT_STRING,
    XSD_DT_TOKEN,
    XSD_DT_DATE,
    XSD_DT_TIME,
    XSD_DT_DATE_TIME,
    XSD_DT_DECIMAL,
    XSD_DT_INT,
    XSD_DT_INTEGER,
    XSD_DT_LONG,
    XSD_DT_NON_NEG_INTEGER,
    XSD_DT_NON_POS_INTEGER,
    XSD_DT_NEG_INTEGER,
    XSD_DT_POS_INTEGER,
    XSD_DT_BOOLEAN,
    XSD_ERROR
};

static const char* DEFAULT_SCHEMA_DIRECTORY(COMPONENTFILES_DIR "/configxml/");
static const char* XSD_ANNOTATION_STR("Annotation");
static const char* XSD_APP_INFO_STR("AppInfo");
static const char* XSD_ATTRIBUTE_STR("Attribute");
static const char* XSD_ATTRIBUTE_ARRAY_STR("AttributeArray");
static const char* XSD_ATTRIBUTE_GROUP_STR("AttributeGroup");
static const char* XSD_ATTRIBUTE_GROUP_ARRAY_STR("AttributeGroupArray");
static const char* XSD_CHOICE_STR("Choice");
static const char* XSD_COMPLEX_CONTENT_STR("ComplexContent");
static const char* XSD_COMPLEX_TYPE_STR("ComplexType");
static const char* XSD_COMPLEX_TYPE_ARRAY_STR("ComplexTypeArray");
static const char* XSD_DOCUMENTATION_STR("Documentation");
static const char* XSD_ELEMENT_STR("Element");
static const char* XSD_ELEMENT_ARRAY_STR("ElementArray");
static const char* XSD_ARRAY_ELEMENT_ARRAY_STR("ArrayOfElementArrays");
static const char* XSD_ERROR_STR("ERROR");
static const char* XSD_ENUMERATION_STR("Enumeration");
static const char* XSD_ENUMERATION_ARRAY_STR("EnumerationArray");
static const char* XSD_EXTENSION_STR("Extension");
static const char* XSD_FIELD_STR("Field");
static const char* XSD_FIELD_ARRAY_STR("FieldArray");
static const char* XSD_FRACTION_DIGITS_STR("FractionDigits");
static const char* XSD_INCLUDE_STR("Include");
static const char* XSD_INCLUDE_ARRAY_STR("IncludeArray");
static const char* XSD_KEY_STR("Key");
static const char* XSD_KEY_ARRAY_STR("KeyArray");
static const char* XSD_KEYREF_STR("KeyRef");
static const char* XSD_KEYREF_ARRAY_STR("KeyRefArray");
static const char* XSD_LENGTH_STR("Length");
static const char* XSD_MIN_INCLUSIVE_STR("MinInclusive");
static const char* XSD_MAX_INCLUSIVE_STR("MaxInclusive");
static const char* XSD_MIN_EXCLUSIVE_STR("MinExclusive");
static const char* XSD_MAX_EXCLUSIVE_STR("MaxExclusive");
static const char* XSD_MIN_LENGTH_STR("MinLength");
static const char* XSD_MAX_LENGTH_STR("MaxLength");
static const char* XSD_PATTERN_STR("Pattern");
static const char* XSD_RESTRICTION_STR("Restriction");
static const char* XSD_SCHEMA_STR("Schema");
static const char* XSD_SELECTOR_STR("Selector");
static const char* XSD_SEQUENCE_STR("Sequence");
static const char* XSD_SIMPLE_CONTENT_STR("SimpleContent");
static const char* XSD_SIMPLE_TYPE_STR("SimpleType");
static const char* XSD_SIMPLE_TYPE_ARRAY_STR("SimpleTypeArray");
static const char* XSD_TOTAL_DIGITS_STR("TotalDigits");
static const char* XSD_UNIQUE_STR("Unique");
static const char* XSD_UNIQUE_ARRAY_STR("UniqueArray");
static const char* XSD_WHITE_SPACE_STR("WhiteSpace");
static const char* XSD_DT_NORMALIZED_STRING_STR("NormalizedString");
static const char* XSD_DT_STRING_STR("String");
static const char* XSD_DT_TOKEN_STR("Token");
static const char* XSD_DT_DATE_STR("Date");
static const char* XSD_DT_TIME_STR("Time");
static const char* XSD_DT_DATE_TIME_STR("DateTime");
static const char* XSD_DT_DECIMAL_STR("Decimal");
static const char* XSD_DT_INTEGER_STR("Integer");
static const char* XSD_DT_INT_STR("Int");
static const char* XSD_DT_LONG_STR("Long");
static const char* XSD_DT_NON_NEG_INTEGER_STR("NonNegativeInteger");
static const char* XSD_DT_NON_POS_INTEGER_STR("NonPositiveInteger");
static const char* XSD_DT_POS_INTEGER_STR("PositiveInteger");
static const char* XSD_DT_NEG_INTEGER_STR("NegativeInteger");
static const char* XSD_DT_BOOLEAN_STR("Boolean");

static const char* XML_ENV_VALUE_OPTIONAL("optional");
static const char* XML_ENV_VALUE_REQUIRED("required");

static const char* XML_ATTR_DEFAULT("@default");
static const char* XML_ATTR_USE("@use");
static const char* XML_ATTR_MINOCCURS("@minOccurs");
static const char* XML_ATTR_BASE("@base");
static const char* XML_ATTR_XPATH("@xpath");
static const char* XML_ATTR_REFER("@refer");

static const char* TAG_VIEWCHILDNODES("viewChildNodes");
static const char* TAG_VIEWTYPE("viewType");
static const char* TAG_TOOLTIP("tooltip");
static const char* TAG_COLINDEX("colIndex");
static const char* TAG_TITLE("title");
static const char* TAG_WIDTH("width");
static const char* TAG_AUTOGENWIZARD("autogenforwizard");
static const char* TAG_AUTOGENDEFAULTVALUE("autogendefaultvalue");
static const char* TAG_AUTOGENDEFAULTVALUEFORMULTINODE("autogendefaultformultinode");
static const char* TAG_XPATH("xpath");
static const char* TAG_DOC_ID("docid");
static const char* TAG_DOC_USE_LINE_BREAK("docuselinebreak");
static const char* TAG_REQUIRED("required");
static const char* TAG_UNBOUNDED("unbounded");

#define TAG_OPTIONAL                   "optional"
#define TAG_REQUIRED                   "required"
#define XML_ATTR_ATTRIBUTEFORMDEFAULT  "@attributeFormDefault"
#define XML_ATTR_ELEMENTFORMDEFAULT    "@elementFormDefault"
#define XML_ATTR_ID                    "@id"
#define XML_ATTR_REF                   "@ref"
#define XML_ATTR_XMLNS_XS              "@xmlns:xs"
#define XML_ATTR_SCHEMA_LOCATION       "@schemaLocation"
#define XML_ATTR_VALUE                 "@value"
#define XML_ATTR_OVERRIDE              "@overide"  // intentionally misspelled
#define XML_ATTR_DEPLOYABLE            "@deployable"

static unsigned int STANDARD_OFFSET_1 = 3;
static unsigned int STANDARD_OFFSET_2 = 6;
static unsigned int STANDARD_OFFSET_3 = 9;

static void quickOutPad(::std::ostream& cout, unsigned int offset)
{
    while(offset > 0)
    {
        cout << " ";
        offset--;
    }
}

static void quickOutPad(::StringBuffer &str, unsigned int offset)
{
    while(offset > 0)
    {
        str.append(" ");
        offset--;
    }
}

static void quickOutHeader(::std::ostream &cout, const char* pLabel, unsigned int offset = 0)
{
    quickOutPad(cout,offset);
    cout << "\033[32m-- " << pLabel << " START" << " --" << "\033[0m" << ::std::endl;
}

static void quickOutFooter(::std::ostream &cout, const char* pLabel, unsigned int offset = 0)
{
    quickOutPad(cout,offset);
    //cout << "<--- FINISH " << pLabel << ::std::endl;
    cout << "\033[31m" << "-- " << pLabel << " FINISH" << " --" << "\033[0m" << ::std::endl;
}

static void quickOut(::std::ostream &cout, const char* pLabel, const char* pValue, unsigned int offset = 0)
{
    if (pLabel && strlen(pValue) > 0)
    {
        quickOutPad(cout,offset+STANDARD_OFFSET_2);
        cout << "\033[34m" << pLabel << ":\t\033[0m" << "\033[34m'\033[0m" << pValue << "\033[34m'" << "\033[0m" << ::std::endl;
    }
}

static void quickOut(::std::ostream &cout, const char* pLabel, int value, unsigned int offset = 0)
{
    quickOutPad(cout,offset);
    cout << pLabel << ": " << value << ::std::endl;
}

static const char* stripTrailingIndex(StringBuffer& strXPath)  // should this replace int CConfigSchemaHelper::stripXPathIndex(StringBuffer &strXPath)?
{
    if (strXPath.length() == 0 || strXPath[strXPath.length()-1] != ']')
        return strXPath.str();

    while (1)
    {
        if (strXPath[strXPath.length()-1] == '[')
        {
            strXPath.setLength(strXPath.length()-1);
            break;
        }
        strXPath.setLength(strXPath.length()-1);
    }

    return strXPath.str();
}

class CXSDNodeBase
{
public:

    CXSDNodeBase(CXSDNodeBase* pParentNode = nullptr, NODE_TYPES eNodeType = XSD_ERROR);

    virtual ~CXSDNodeBase();

    GETTERSETTER(XSDXPath)
    GETTERSETTER(EnvXPath)
    //GETTERSETTER(EnvValueFromXML)
    virtual const char* getEnvValueFromXML() const
    {
        return m_strEnvValueFromXML;
    }

    virtual bool setEnvValueFromXML(const char* p)
    {
        if (p != nullptr)
        {
            m_strEnvValueFromXML.set(p);
            return true;
        }
        return false;
    }

    void dumpStdOut() const;

    virtual CXSDNodeBase* getParentNode() const
    {
        return m_pParentNode;
    }

    virtual const CXSDNodeBase* getConstAncestorNode(unsigned iLevel) const;
    virtual const CXSDNodeBase* getConstParentNode() const
    {
        return m_pParentNode;
    }

    virtual const CXSDNodeBase* getParentNodeByType(NODE_TYPES eNodeType[], const CXSDNodeBase *pParent = nullptr, int length = 1) const;
    virtual const CXSDNodeBase* getParentNodeByType(NODE_TYPES eNodeType, const CXSDNodeBase *pParent = nullptr) const;
    virtual const CXSDNodeBase* getNodeByTypeAndNameAscending(NODE_TYPES eNodeType[], const char *pName, int length = 1) const;
    virtual const CXSDNodeBase* getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const
    {
        if (this->getNodeType() == eNodeType)
            return this;
        else
            return this->getConstParentNode()->getNodeByTypeAndNameAscending(eNodeType, pName);
    }
    virtual const CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType[], const char *pName, int length = 1) const;
    virtual const CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
    {
        return getNodeByTypeAndNameDescending(&eNodeType, pName);
    }
    void setParentNode(CXSDNodeBase *pParentNode)
    {
        if (m_pParentNode == nullptr)  // Should only be set once, otherwise it's an external schema and should have parent set
        {
            m_pParentNode = pParentNode;
        }
    }
    const char* getNodeTypeStr() const
    {
        return m_pNodeType;
    }
    virtual NODE_TYPES getNodeType() const
    {
        return m_eNodeType;
    }
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const = 0;

    virtual const char* getXML(const char* /*pComponent*/)
    {
        return nullptr;
    }

    virtual void getDocumentation(::StringBuffer &strDoc) const = 0;

    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1)
    {
    }

    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
    {
    }

protected:

    CXSDNodeBase*               m_pParentNode;
    ::StringBuffer                m_strXML;
    NODE_TYPES                  m_eNodeType;
    char                        m_pNodeType[1024];
    StringBuffer                m_strEnvValueFromXML;

private:

};

class CXSDNode : public CInterface, public CXSDNodeBase
{
public:

    IMPLEMENT_IINTERFACE

    CXSDNode(CXSDNodeBase *pParentNode, NODE_TYPES pNodeType = XSD_ERROR );

    virtual bool checkSelf(NODE_TYPES eNodeType, const char *pName, const char* pCompName) const;
    virtual const CXSDNodeBase* getParentNodeByType(NODE_TYPES eNodeType) const;

private:
};

template<class T>
class CXSDNodeWithRestrictions : public CXSDNode
{

public:

    CXSDNodeWithRestrictions(CXSDNodeBase* pParentNode, enum NODE_TYPES eNodeType) : CXSDNode::CXSDNode(pParentNode)
    {
    }

    static T* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath)
    {
        assert(pSchemaRoot != nullptr);

        if (pSchemaRoot == nullptr)
            return nullptr;

        T *pT = nullptr;

        if (xpath != nullptr && *xpath != 0)
        {
            ::IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

            if (pTree == nullptr)
                return nullptr;

            pT = new T(pParentNode);

            const char* pValue = pTree->queryProp(XML_ATTR_VALUE);

            if (pValue != nullptr && *pValue != 0)
                pT->setValue(pValue);
            else
            {
                assert(!"No Value set");
                //TODO: throw? and delete?
            }
            pT->setXSDXPath(xpath);
        }
        return pT;
    }

    void dump(::std::ostream& cout, unsigned int offset) const
    {
        offset += STANDARD_OFFSET_1;

        quickOutHeader(cout, XSD_MIN_INCLUSIVE_STR, offset);
        QUICK_OUT(cout, Value, offset);
        quickOutFooter(cout, XSD_MIN_INCLUSIVE_STR, offset);
    }

    virtual void getDocumentation(::StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }

    virtual const char* getXML(const char* /*pComponent*/)
    {
        UNIMPLEMENTED;
        return nullptr;
    }

    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1)
    {
        UNIMPLEMENTED;
    }

    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
    {
        UNIMPLEMENTED;
    }

    GETTERSETTER(Value)
};

class CXSDNodeWithType : public CXSDNode
{
    GETTERSETTER(Type)

public:

    CXSDNodeWithType(CXSDNodeBase* pParentNode, enum NODE_TYPES eNodeType) : CXSDNode::CXSDNode(pParentNode, eNodeType), m_pXSDNode(nullptr)
    {
    }

    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
    {
        UNIMPLEMENTED_X("Should be implemented in the derived class");
    }

    void setTypeNode(CXSDNode* pCXSDNode)
    {
        m_pXSDNode = pCXSDNode;
    }

    const CXSDNode* getTypeNode() const
    {
        return m_pXSDNode;
    }

protected:

    CXSDNode *m_pXSDNode;
};

class CXSDNodeWithBase : public CXSDNode
{
    GETTERSETTER(Base)

public:

    CXSDNodeWithBase(CXSDNodeBase* pParentNode, enum NODE_TYPES eNodeType) : CXSDNode::CXSDNode(pParentNode, eNodeType), m_pXSDNode(nullptr)
    {
    }

    void setBaseNode(CXSDNodeBase* pCXSDNode)
    {
        m_pXSDNode = pCXSDNode;
    }

    const CXSDNodeBase* getBaseNode() const
    {
        return m_pXSDNode;
    }

protected:

    CXSDNodeBase *m_pXSDNode;
};

class CXSDBuiltInDataType : public CXSDNode
{
public:

    static CXSDBuiltInDataType* create(CXSDNodeBase* pParentNode, const char* pNodeType);

    virtual ~CXSDBuiltInDataType();
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const;
    virtual bool checkConstraint(const char *pValue) const;

private:

    CXSDBuiltInDataType(CXSDNodeBase* pParentNode = nullptr, NODE_TYPES eNodeType = XSD_ERROR);
    CXSDBuiltInDataType(CXSDNodeBase* pParentNode, const char* pNodeType);
};

}

#endif // _SCHEMA_COMMON_HPP_
