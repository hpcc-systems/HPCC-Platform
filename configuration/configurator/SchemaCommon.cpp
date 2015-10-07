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

#include "SchemaCommon.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaMapManager.hpp"
#include "jregexp.hpp"
#include <cstring>

CXSDNodeBase::CXSDNodeBase(CXSDNodeBase* pParentNode, NODE_TYPES eNodeType) : m_pParentNode(pParentNode),  m_eNodeType(eNodeType), m_eUIType(QML_UI_UNKNOWN)
{
   assert(eNodeType != XSD_ERROR);

   switch (eNodeType)
   {
   case(XSD_ERROR):
       strcpy(m_pNodeType, XSD_ERROR_STR);
       break;
   case(XSD_ANNOTATION):
       strcpy(m_pNodeType, XSD_ANNOTATION_STR);
       break;
   case(XSD_APP_INFO):
       strcpy(m_pNodeType, XSD_APP_INFO_STR);
       break;
   case(XSD_ATTRIBUTE):
       strcpy(m_pNodeType, XSD_ATTRIBUTE_STR);
       break;
   case(XSD_ATTRIBUTE_ARRAY):
       strcpy(m_pNodeType, XSD_ATTRIBUTE_ARRAY_STR);
       break;
   case(XSD_ATTRIBUTE_GROUP):
       strcpy(m_pNodeType, XSD_ATTRIBUTE_GROUP_STR);
       break;
   case(XSD_ATTRIBUTE_GROUP_ARRAY):
       strcpy(m_pNodeType, XSD_ATTRIBUTE_GROUP_ARRAY_STR);
       break;
   case(XSD_CHOICE):
       strcpy(m_pNodeType, XSD_CHOICE_STR);
       break;
   case(XSD_COMPLEX_CONTENT):
       strcpy(m_pNodeType, XSD_COMPLEX_CONTENT_STR);
       break;
   case(XSD_COMPLEX_TYPE):
       strcpy(m_pNodeType, XSD_COMPLEX_TYPE_STR);
       break;
   case(XSD_COMPLEX_TYPE_ARRAY):
       strcpy(m_pNodeType, XSD_COMPLEX_TYPE_ARRAY_STR);
       break;
   case(XSD_DOCUMENTATION):
       strcpy(m_pNodeType, XSD_DOCUMENTATION_STR);
       break;
   case(XSD_ELEMENT):
       strcpy(m_pNodeType, XSD_ELEMENT_STR);
       break;
   case(XSD_ELEMENT_ARRAY):
       strcpy(m_pNodeType, XSD_ELEMENT_ARRAY_STR);
       break;
   case(XSD_EXTENSION):
       strcpy(m_pNodeType, XSD_EXTENSION_STR);
       break;
   case(XSD_FIELD):
       strcpy(m_pNodeType, XSD_FIELD_STR);
       break;
   case(XSD_FIELD_ARRAY):
       strcpy(m_pNodeType, XSD_FIELD_ARRAY_STR);
       break;
   case(XSD_KEY):
       strcpy(m_pNodeType, XSD_KEY_STR);
       break;
   case(XSD_KEY_ARRAY):
       strcpy(m_pNodeType, XSD_KEY_ARRAY_STR);
       break;
   case(XSD_KEYREF):
       strcpy(m_pNodeType, XSD_KEYREF_STR);
       break;
   case(XSD_KEYREF_ARRAY):
       strcpy(m_pNodeType, XSD_KEYREF_ARRAY_STR);
       break;
   case(XSD_INCLUDE):
       strcpy(m_pNodeType, XSD_INCLUDE_STR);
       break;
   case(XSD_INCLUDE_ARRAY):
       strcpy(m_pNodeType, XSD_INCLUDE_ARRAY_STR);
       break;
   case(XSD_RESTRICTION):
       strcpy(m_pNodeType, XSD_RESTRICTION_STR);
       break;
   case(XSD_SCHEMA):
       strcpy(m_pNodeType, XSD_SCHEMA_STR);
       break;
   case(XSD_SEQUENCE):
       strcpy(m_pNodeType, XSD_SEQUENCE_STR);
       break;
   case(XSD_SIMPLE_TYPE):
       strcpy(m_pNodeType, XSD_SIMPLE_TYPE_STR);
       break;
   case(XSD_SIMPLE_TYPE_ARRAY):
       strcpy(m_pNodeType, XSD_SIMPLE_TYPE_ARRAY_STR);
       break;
   case(XSD_ENUMERATION):
       strcpy(m_pNodeType, XSD_ENUMERATION_STR);
       break;
   case(XSD_ENUMERATION_ARRAY):
       strcpy(m_pNodeType, XSD_ENUMERATION_ARRAY_STR);
       break;
   case(XSD_LENGTH):
       strcpy(m_pNodeType, XSD_LENGTH_STR);
       break;
   case(XSD_FRACTION_DIGITS):
       strcpy(m_pNodeType, XSD_FRACTION_DIGITS_STR);
       break;
   case(XSD_MAX_EXCLUSIVE):
       strcpy(m_pNodeType, XSD_MAX_EXCLUSIVE_STR);
       break;
   case(XSD_MAX_INCLUSIVE):
       strcpy(m_pNodeType, XSD_MAX_INCLUSIVE_STR);
       break;
   case(XSD_MIN_EXCLUSIVE):
       strcpy(m_pNodeType, XSD_MIN_INCLUSIVE_STR);
       break;
   case(XSD_MIN_LENGTH):
       strcpy(m_pNodeType, XSD_MIN_LENGTH_STR);
       break;
   case(XSD_MAX_LENGTH):
       strcpy(m_pNodeType, XSD_MAX_LENGTH_STR);
       break;
   case(XSD_PATTERN):
       strcpy(m_pNodeType, XSD_PATTERN_STR);
       break;
   case(XSD_SELECTOR):
       strcpy(m_pNodeType, XSD_SELECTOR_STR);
       break;
   case(XSD_TOTAL_DIGITS):
       strcpy(m_pNodeType, XSD_TOTAL_DIGITS_STR);
       break;
   case(XSD_WHITE_SPACE):
       strcpy(m_pNodeType, XSD_WHITE_SPACE_STR);
       break;
   case(XSD_DT_NORMALIZED_STRING):
       strcpy(m_pNodeType, XSD_DATA_TYPE_NORMALIZED_STRING);
       break;
   case(XSD_DT_STRING):
       strcpy(m_pNodeType, XSD_DATA_TYPE_STRING);
       break;
   case(XSD_DT_TOKEN):
       strcpy(m_pNodeType, XSD_DATA_TYPE_TOKEN);
       break;
   case(XSD_DT_DATE):
       strcpy(m_pNodeType, XSD_DATA_TYPE_DATE);
       break;
   case(XSD_DT_TIME):
       strcpy(m_pNodeType, XSD_DATA_TYPE_TIME);
       break;
   case(XSD_DT_DATE_TIME):
       strcpy(m_pNodeType, XSD_DATA_TYPE_DATE_TIME);
       break;
   case(XSD_DT_DECIMAL):
       strcpy(m_pNodeType, XSD_DATA_TYPE_DECIMAL);
       break;
   case(XSD_DT_INT):
       strcpy(m_pNodeType, XSD_DATA_TYPE_INT);
       break;
   case(XSD_DT_INTEGER):
       strcpy(m_pNodeType, XSD_DATA_TYPE_INTEGER);
       break;
   case(XSD_DT_LONG):
       strcpy(m_pNodeType, XSD_DATA_TYPE_LONG);
       break;
   case(XSD_DT_NON_NEG_INTEGER):
       strcpy(m_pNodeType, XSD_DATA_TYPE_NON_NEGATIVE_INTEGER);
       break;
   case(XSD_DT_NON_POS_INTEGER):
       strcpy(m_pNodeType, XSD_DATA_TYPE_NON_POSITIVE_INTEGER);
       break;
   case(XSD_DT_NEG_INTEGER):
       strcpy(m_pNodeType, XSD_DATA_TYPE_NEGATIVE_INTEGER);
       break;
   case(XSD_DT_POS_INTEGER):
       strcpy(m_pNodeType, XSD_DATA_TYPE_POSITIVE_INTEGER);
       break;
   case(XSD_DT_BOOLEAN):
       strcpy(m_pNodeType, XSD_DATA_TYPE_BOOLEAN);
       break;
   default:
       assert(!"Unknown XSD Type"); // should never get here
       strcpy(m_pNodeType, XSD_ERROR_STR);
       break;
   }
}

CXSDNodeBase::~CXSDNodeBase()
{
}

void CXSDNodeBase::dumpStdOut() const
{
   dump(std::cout);
}

const CXSDNodeBase* CXSDNodeBase::getConstAncestorNode(unsigned iLevel) const
{
   CXSDNodeBase *pAncestorNode = const_cast<CXSDNodeBase*>(this);

   if (iLevel == 0)
       return this;

   do
   {
       pAncestorNode = const_cast<CXSDNodeBase*>(pAncestorNode->getConstParentNode());
       iLevel--;
   } while (iLevel > 0 && pAncestorNode != NULL);

   return pAncestorNode;
}

const CXSDNodeBase* CXSDNodeBase::getParentNodeByType(NODE_TYPES eNodeType[], const CXSDNodeBase *pParent, int length) const
{
   for (int i = 0; i < length; i++)
   {
       if (this->m_eNodeType == eNodeType[i] && pParent != NULL)
           return this;
   }
   if (this->getConstParentNode() != NULL)
       return this->getConstParentNode()->getParentNodeByType(eNodeType, this, length);

   return NULL;
}

CXSDNode::CXSDNode(CXSDNodeBase *pParentNode, NODE_TYPES pNodeType) : CXSDNodeBase::CXSDNodeBase(pParentNode, pNodeType)
{
}

bool CXSDNode::checkSelf(NODE_TYPES eNodeType, const char *pName, const char* pCompName) const
{
  if (eNodeType & this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
  {
      assert(pName != NULL); // for now pName should always be populated
      return this;
  }
  return NULL;
}

const CXSDNodeBase* CXSDNode::getParentNodeByType(NODE_TYPES eNodeType) const
{
  if (this->m_eNodeType == eNodeType)
      return this;
  if (this->getConstParentNode() != NULL)
      return this->getConstParentNode()->getParentNodeByType(eNodeType);
  return NULL;
}

const CXSDNodeBase* CXSDNodeBase::getNodeByTypeAndNameAscending(NODE_TYPES eNodeType[], const char *pName, int length) const
{
  for (int i = 0; i < length; i++)
  {
    assert(this->m_eNodeType != eNodeType[i]);

    if (this->getConstParentNode() != NULL)
        return this->getConstParentNode()->getNodeByTypeAndNameAscending(eNodeType[i], pName);
  }
  return NULL;
}

const CXSDNodeBase* CXSDNodeBase::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType[], const char *pName, int length) const
{
    assert(false);  // Derived classes need to hande this
    return NULL;
}

const CXSDNodeBase* CXSDNodeBase::getParentNodeByType(NODE_TYPES eNodeType, const CXSDNodeBase *pParent) const
{
    if (this->getConstParentNode() == NULL)
        return NULL;
    else if (this->getConstParentNode()->getNodeType() == eNodeType)
        return this->getConstParentNode();
    else
        return this->getConstParentNode()->getParentNodeByType(eNodeType, pParent);
}

CXSDBuiltInDataType* CXSDBuiltInDataType::create(CXSDNodeBase* pParentNode, const char* pNodeType)
{
    assert(pParentNode != NULL);

    enum NODE_TYPES eNodeType = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getEnumFromTypeName(pNodeType);

    if (eNodeType != XSD_ERROR)
        return new CXSDBuiltInDataType(pParentNode, eNodeType);
    else
        return NULL;
}

CXSDBuiltInDataType::CXSDBuiltInDataType(CXSDNodeBase* pParentNode, enum NODE_TYPES eNodeType) : CXSDNode::CXSDNode(pParentNode, eNodeType)
{
    assert(eNodeType != XSD_ERROR);
}

CXSDBuiltInDataType::~CXSDBuiltInDataType()
{
}

void CXSDBuiltInDataType::dump(std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    const char *pTypeNameString = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getTypeNameFromEnum(this->getNodeType(), true);

    QuickOutHeader(cout, pTypeNameString, offset);
    QuickOutFooter(cout, pTypeNameString, offset);
}

void CXSDBuiltInDataType::getDocumentation(StringBuffer &strDoc) const
{
    UNIMPLEMENTED;
}

bool CXSDBuiltInDataType::checkConstraint(const char *pValue) const
{
    if (pValue == NULL || *pValue == 0)
        return true;

    enum NODE_TYPES eNodeType = this->getNodeType();

    if (eNodeType >= XSD_DT_NORMALIZED_STRING && eNodeType < XSD_ERROR)
    {
        if (XSD_DT_NORMALIZED_STRING == eNodeType)
        {
            const char key[] = "\n\r\t";
            if (strpbrk(pValue, key) != NULL)
                return false;
        }
        else if (XSD_DT_STRING == eNodeType)
        {
            // all allowed
        }
        else if(XSD_DT_TOKEN == eNodeType)
        {
            const char key[] = "\n\r\t";
            if (strpbrk(pValue, key) != NULL)
                return false;
            if (pValue[0] == ' ' || pValue[strlen(pValue)-1] == ' ' || strstr(pValue, "  ")); // leading/trailing space multiple spaces
                return false;
        }
        else if(XSD_DT_DATE == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_TIME == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_DATE_TIME == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_DECIMAL == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_INT == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_INTEGER == eNodeType)
        {
            RegExpr expr("^(\\+|-)?\\d+$");

            if ((expr.find(pValue)) && (expr.findlen(0) == strlen(pValue)) == false)
                return false;
        }
        else if(XSD_DT_LONG == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_NON_NEG_INTEGER == eNodeType)
        {
            RegExpr expr("^\\d+$");

            if ((expr.find(pValue)) && (expr.findlen(0) == strlen(pValue)) == false)
                return false;
        }
        else if(XSD_DT_NON_POS_INTEGER == eNodeType)
        {
            RegExpr expr("^\\d-$");

            if ((expr.find(pValue)) && (expr.findlen(0) == strlen(pValue)) == false)
                return false;
        }
        else if(XSD_DT_NEG_INTEGER == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_POS_INTEGER == eNodeType)
        {
            UNIMPLEMENTED;
        }
        else if(XSD_DT_BOOLEAN == eNodeType)
        {
            if (stricmp(pValue,"true") != 0 && stricmp(pValue,"false") != 0)
                return false;
            else
                assert("!Unknown datatype");
        }
    }
    return true;
}
