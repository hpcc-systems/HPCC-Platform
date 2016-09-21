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
#include "SchemaSelector.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaMapManager.hpp"
#include "SchemaKeyRef.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree
#define StringBuffer ::StringBuffer

CKeyRef* CKeyRef::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    assert(pParentNode != nullptr);
    assert(pParentNode->getNodeType() == XSD_KEYREF_ARRAY);

    if (pSchemaRoot == nullptr || pParentNode == nullptr)
    {
        // TODO: Throw Exception
        return nullptr;
    }

    CKeyRef *pKeyRef = new CKeyRef(pParentNode);

    if (xpath != nullptr && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);
        if (pTree == nullptr)
            return nullptr; // no xs:KeyRef

        const char* pName = pTree->queryProp(XML_ATTR_NAME);
        const char* pRefer = pTree->queryProp(XML_ATTR_REFER);

        if (pName != nullptr && pRefer != nullptr)
        {
            pKeyRef->setXSDXPath(xpath);
            pKeyRef->setName(pName);
        }
        else
        {
            delete pKeyRef;
            pKeyRef = nullptr;
            assert(!"value attribute can be empty!");
            // TODO: throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
        }

        const char *pID = pTree->queryProp(XML_ATTR_ID);
        if (pID != nullptr)
            pKeyRef->setID(pID);

        StringBuffer strXPathExt(xpath);
        strXPathExt.append("/").append(XSD_TAG_FIELD);
        pKeyRef->m_pFieldArray = CFieldArray::load(pKeyRef, pSchemaRoot, strXPathExt.str());

        strXPathExt.set(xpath);
        strXPathExt.append("/").append(XSD_TAG_SELECTOR);
        pKeyRef->m_pSelector = CSelector::load(pKeyRef, pSchemaRoot, strXPathExt.str());
    }
    return pKeyRef;
}

void CKeyRef::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_KEYREF_STR, offset);
    QUICK_OUT(cout, Name, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, Refer, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pFieldArray != nullptr)
        m_pFieldArray->dump(cout, offset);

    if (m_pSelector != nullptr)
        m_pSelector->dump(cout, offset);

    quickOutFooter(cout, XSD_KEYREF_STR, offset);
}

bool CKeyRef::checkConstraint(const char *pValue) const
{
    assert (pValue != nullptr);

    if (pValue == nullptr)
        return true;
    else
    {
        StringBuffer strQName(this->getXSDXPath());
        strQName.append("/").append(this->getRefer());

        CKey *pKey = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getKeyFromXSDXPath(strQName.str());

        assert(pKey);
        if (pKey == nullptr)
            return true;
        return pKey->checkConstraint(pValue);
    }
}

void CKeyRef::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(this->m_pSelector != nullptr);

    this->setEnvXPath(strXPath.str());

    if (this->m_pSelector != nullptr)
    {
        this->m_pSelector->populateEnvXPath(strXPath.str());
        CConfigSchemaHelper::getInstance()->addKeyRefForReverseAssociation(this);
    }
}

CKeyRefArray* CKeyRefArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    assert(pParentNode->getNodeType() == XSD_ELEMENT);

    if (pSchemaRoot == nullptr || xpath == nullptr)
        return nullptr;

    StringBuffer strXPathExt(xpath);

    CKeyRefArray *pKeyRefArray = new CKeyRefArray(pParentNode);
    pKeyRefArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.setf("%s[%d]", xpath, count);

        CKeyRef *pKeyRef = CKeyRef::load(pKeyRefArray, pSchemaRoot, strXPathExt.str());

        if (pKeyRef != nullptr)
            pKeyRefArray->append(*pKeyRef);

        count++;
    }

    if (pKeyRefArray->length() == 0)
    {
        delete pKeyRefArray;
        pKeyRefArray = nullptr;
    }
    return pKeyRefArray;
}

void CKeyRefArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_KEYREF_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    quickOutFooter(cout, XSD_KEYREF_ARRAY_STR, offset);
}

void CKeyRefArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);
    QUICK_ENV_XPATH_WITH_INDEX(strXPath, index)
}
