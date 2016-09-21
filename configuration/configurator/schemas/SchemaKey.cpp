/*##############################################################################
 *

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

#include "jptree.hpp"
#include "SchemaKey.hpp"
#include "SchemaSelector.hpp"
#include "SchemaField.hpp"
#include "SchemaAnnotation.hpp"
#include "SchemaCommon.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaMapManager.hpp"
#include "SchemaAttributes.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree

CKey* CKey::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    assert(pParentNode != nullptr);
    assert(pParentNode->getNodeType() == XSD_KEY_ARRAY);

    if (pSchemaRoot == nullptr || pParentNode == nullptr)
    {
        // TODO: Throw Exception
        return nullptr;
    }

    CKey *pKey = new CKey(pParentNode);

    if (xpath != nullptr && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == nullptr)
            return nullptr; // no xs:key

        const char* pName = pTree->queryProp(XML_ATTR_NAME);
        if (pName != nullptr)
         {
             pKey->setXSDXPath(xpath);
             pKey->setName(pName);
         }
         else
         {
             assert(!"value attribute can be empty!");
             delete pKey;
             // TODO: throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
             return nullptr;
         }

         const char *pID = pTree->queryProp(XML_ATTR_ID);
         if (pID != nullptr)
             pKey->setID(pID);

         StringBuffer strXPathExt(xpath);
         strXPathExt.append("/").append(XSD_TAG_FIELD);

         if (strXPathExt.charAt(0) == '@')
             strXPathExt.remove(0,1); // remove '@'

         pKey->m_pFieldArray = CFieldArray::load(pKey, pSchemaRoot, strXPathExt.str());

         strXPathExt.set(xpath);
         strXPathExt.append("/").append(XSD_TAG_SELECTOR);

         if (strXPathExt.charAt(0) == '.')
             strXPathExt.remove(0,2); // remove leading ./

         pKey->m_pSelector = CSelector::load(pKey, pSchemaRoot, strXPathExt.str());
         assert(pKey->m_pFieldArray != nullptr && pKey->m_pSelector != nullptr);

         strXPathExt.append("/").append(XSD_TAG_ANNOTATION);
         pKey->m_pAnnotation = CAnnotation::load(pKey, pSchemaRoot, strXPathExt.str());
    }
    return pKey;
}

void CKey::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(this->m_pSelector != nullptr);

    this->setEnvXPath(strXPath.str());

    if (this->m_pSelector != nullptr)
    {
        this->m_pSelector->populateEnvXPath(strXPath.str());
        CConfigSchemaHelper::getInstance()->addKeyForReverseAssociation(this);
    }
}

bool CKey::checkConstraint(const char *pValue) const
{
    bool bRetVal = true;
    auto fieldArraylength = m_pFieldArray->length();

    if (m_pSelector != nullptr && fieldArraylength != 0)
    {
        for (int idx = 0; idx < fieldArraylength; idx++)
        {
            assert(!"Multiple fields not implemented");
            CField *m_pField = &(m_pFieldArray->item(idx));

            assert(m_pField != nullptr);
            if (m_pField == nullptr)
                return false;

            StringBuffer strXPathForConstraintCheck(this->getEnvXPath());
            strXPathForConstraintCheck.appendf("/%s", this->m_pSelector->getXPath());

            const CElement *pElement = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getElementFromXPath(strXPathForConstraintCheck.str());
            if (pElement == nullptr)
                return false;

            const CAttribute *pAttribute = dynamic_cast<const CAttribute*>(pElement->getNodeByTypeAndNameDescending(XSD_ATTRIBUTE, m_pField->getXPath()));  // needs to be first possible descendent
            if (pAttribute != nullptr && pAttribute->getParentNodeByType(XSD_ELEMENT) != pElement)
            {
                assert(!"Could not find match for key");
            }
        }
    }
    return bRetVal;
}

void CKey::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_KEY_STR, offset);
    QUICK_OUT(cout, Name, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pFieldArray != nullptr)
        m_pFieldArray->dump(cout, offset);
    if (m_pSelector != nullptr)
        m_pSelector->dump(cout, offset);

    quickOutFooter(cout, XSD_KEY_STR, offset);
}

CKeyArray* CKeyArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    assert(pParentNode->getNodeType() == XSD_ELEMENT);

    if (pSchemaRoot == nullptr || xpath == nullptr)
        return nullptr;

    StringBuffer strXPathExt(xpath);

    CKeyArray *pKeyArray = new CKeyArray(pParentNode);
    pKeyArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.clear().append(xpath).appendf("[%d]",count);

        CKey *pKey = CKey::load(pKeyArray, pSchemaRoot, strXPathExt.str());
        if (pKey != nullptr)
            pKeyArray->append(*pKey);

        count++;
    }

    if (pKeyArray->length() == 0)
    {
        delete pKeyArray;
        pKeyArray = nullptr;
    }
    return pKeyArray;
}

bool CKeyArray::checkConstraint(const char *pValue) const
{
    assert(pValue != nullptr);

    if (pValue == nullptr)
        return false;

    for (int idx = 0; idx < this->length(); idx++)
    {
        if ((this->item(idx)).checkConstraint(pValue) == false)
            return false;
    }
    return true;
}

void CKeyArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_KEY_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    quickOutFooter(cout, XSD_KEY_ARRAY_STR, offset);
}

void CKeyArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);
    QUICK_ENV_XPATH_WITH_INDEX(strXPath, index)
}
