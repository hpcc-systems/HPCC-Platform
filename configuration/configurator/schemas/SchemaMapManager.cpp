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

#include "SchemaMapManager.hpp"
#include "SchemaAll.hpp"

using namespace CONFIGURATOR;

CSchemaMapManager::CSchemaMapManager()
{
    m_pSchemaPtrMap.setown(new MapStringToCSchema());
    m_pSimpleTypePtrMap.setown(new MapStringToCSimpleType());
    m_pComplexTypePtrsMap.setown (new MapStringToCComplexType);
    m_pAttributeGroupTypePtrsMap.setown(new MapStringToCAttributeGroup);
    m_pAttributePtrsMap.setown(new MapStringToCAttribute);
    m_pRestrictionPtrsMap.setown(new MapStringToCRestriction);
    m_pElementPtrsMap.setown(new MapStringToCElement);
    m_pElementNamePtrsMap.setown(new MapStringToCElement);
    m_pXSDToElementPtrsMap.setown(new MapStringToCElement);
    m_pElementArrayPtrsMap.setown(new MapStringToCElementArray);
    m_pStringToEnumMap.setown(new MapStringToNodeTypeEnum);
    m_pStringToKeyPtrsMap.setown(new MapStringToCKey);
    m_pStringToNodeBaseMap.setown(new MapStringToCNodeBase);

    m_enumArray[XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_NORMALIZED_STRING;
    m_enumArray[XSD_DT_NORMALIZED_STRING][1] = XSD_DT_NORMALIZED_STRING_STR;

    m_enumArray[XSD_DT_STRING-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_STRING;
    m_enumArray[XSD_DT_STRING-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_STRING_STR;

    m_enumArray[XSD_DT_TOKEN-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_TOKEN;
    m_enumArray[XSD_DT_TOKEN-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_TOKEN_STR;

    m_enumArray[XSD_DT_DATE-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_DATE;
    m_enumArray[XSD_DT_DATE-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_DATE_STR;

    m_enumArray[XSD_DT_TIME-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_TIME;
    m_enumArray[XSD_DT_TIME-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_TIME_STR;

    m_enumArray[XSD_DT_DATE_TIME-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_DATE_TIME;
    m_enumArray[XSD_DT_DATE_TIME-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_DATE_TIME_STR;

    m_enumArray[XSD_DT_DECIMAL-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_DECIMAL;
    m_enumArray[XSD_DT_DECIMAL-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_DECIMAL_STR;

    m_enumArray[XSD_DT_INTEGER-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_INTEGER;
    m_enumArray[XSD_DT_INTEGER-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_INTEGER_STR;

    m_enumArray[XSD_DT_INT-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_INT;
    m_enumArray[XSD_DT_INT-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_INT_STR;

    m_enumArray[XSD_DT_LONG-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_NORMALIZED_STRING;
    m_enumArray[XSD_DT_LONG-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_LONG_STR;

    m_enumArray[XSD_DT_NON_NEG_INTEGER-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_NORMALIZED_STRING;
    m_enumArray[XSD_DT_NON_NEG_INTEGER-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_NON_NEG_INTEGER_STR;

    m_enumArray[XSD_DT_NON_POS_INTEGER-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_NORMALIZED_STRING;
    m_enumArray[XSD_DT_NON_POS_INTEGER-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_NON_POS_INTEGER_STR;

    m_enumArray[XSD_DT_POS_INTEGER-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_NORMALIZED_STRING;
    m_enumArray[XSD_DT_POS_INTEGER-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_POS_INTEGER_STR;

    m_enumArray[XSD_DT_NEG_INTEGER-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_NORMALIZED_STRING;
    m_enumArray[XSD_DT_NEG_INTEGER-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_NEG_INTEGER_STR;

    m_enumArray[XSD_DT_BOOLEAN-XSD_DT_NORMALIZED_STRING][0] = XSD_DATA_TYPE_BOOLEAN;
    m_enumArray[XSD_DT_BOOLEAN-XSD_DT_NORMALIZED_STRING][1] = XSD_DT_BOOLEAN_STR;

    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_NORMALIZED_STRING, XSD_DT_NORMALIZED_STRING);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_STRING, XSD_DT_STRING);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_TOKEN, XSD_DT_TOKEN);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_DATE, XSD_DT_DATE);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_TIME, XSD_DT_TIME);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_DATE_TIME, XSD_DT_DATE_TIME);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_DECIMAL, XSD_DT_INTEGER);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_DECIMAL, XSD_DT_DECIMAL);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_LONG, XSD_DT_LONG);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_NON_NEGATIVE_INTEGER, XSD_DT_NON_NEG_INTEGER);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_NON_POSITIVE_INTEGER, XSD_DT_NON_POS_INTEGER);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_NEGATIVE_INTEGER, XSD_DT_POS_INTEGER);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_POSITIVE_INTEGER, XSD_DT_NEG_INTEGER);
    m_pStringToEnumMap->setValue(XSD_DATA_TYPE_BOOLEAN, XSD_DT_BOOLEAN);


    // exclude node processing of types added here
    addExludedNode(XSD_EXTENSION);
}

CSchemaMapManager::~CSchemaMapManager()
{
}

CSchema* CSchemaMapManager::getSchemaForXSD(const char* pComponent)
{
    CSchema **pSchema = m_pSchemaPtrMap->getValue(pComponent);

    if (pSchema != nullptr )
        return *pSchema;
    else
        return nullptr;
}

void CSchemaMapManager::setSchemaForXSD(const char* pComponent, CSchema *pSchema)
{
    if (pSchema != nullptr && pComponent != nullptr && *pComponent != 0)
    {
        if (m_pSchemaPtrMap->getValue(pComponent) == nullptr)
            m_pSchemaPtrMap->setValue(pComponent, (pSchema));
    }
}

CSimpleType* CSchemaMapManager::getSimpleTypeWithName(const char* pName)
{
    assert(pName != nullptr);
    if (pName == nullptr)
        return nullptr;

    CSimpleType **ppSimpleType = nullptr;
    ppSimpleType = m_pSimpleTypePtrMap->getValue(pName);

    if (ppSimpleType != nullptr)
        return *ppSimpleType;
    else
        return nullptr;
}

void CSchemaMapManager::setSimpleTypeWithName(const char* pName, CSimpleType *pSimpleType)
{
    assert (pSimpleType != nullptr);
    if (pName == nullptr || pSimpleType == nullptr)
        return;
    if (m_pSimpleTypePtrMap->getValue(pName) != nullptr)
        throw MakeExceptionFromMap(EX_STR_SIMPLE_TYPE_ALREADY_DEFINED);

    assert(pSimpleType->getLinkCount() == 1);
    m_pSimpleTypePtrMap->setValue(pName, pSimpleType);
}

CComplexType* CSchemaMapManager::getComplexTypeWithName(const char* pName)
{
    assert(pName != nullptr);
    if (pName == nullptr)
        return nullptr;

    CComplexType **ppComplexType = nullptr;
    ppComplexType = (m_pComplexTypePtrsMap->getValue(pName));

    return (ppComplexType != nullptr ? *ppComplexType : nullptr);
}

void CSchemaMapManager::setComplexTypeWithName(const char* pName, CComplexType *pComplexType)
{
    assert (pComplexType != nullptr);
    if (pName == nullptr || pComplexType == nullptr)
        return;

    if (m_pComplexTypePtrsMap->getValue(pName) != nullptr)
        throw MakeExceptionFromMap(EX_STR_COMPLEX_TYPE_ALREADY_DEFINED);

    assert(pComplexType->getLinkCount() == 1);
    m_pComplexTypePtrsMap->setValue(pName, pComplexType);
}

CComplexType* CSchemaMapManager::getComplexTypeFromXPath(const char *pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);

    CComplexType** ppComplexType =  m_pComplexTypePtrsMap->getValue(pXPath);
    if (ppComplexType != nullptr)
        return *ppComplexType;
    else
        return nullptr;
}

CAttributeGroup* CSchemaMapManager::getAttributeGroup(const char* pName)
{
    assert(pName != nullptr);
    if (pName == nullptr)
        return nullptr;

    CAttributeGroup *pAttributeGroup = nullptr;
    pAttributeGroup = *(m_pAttributeGroupTypePtrsMap->getValue(pName));

    assert(pAttributeGroup != nullptr);
    return pAttributeGroup;
}

void CSchemaMapManager::setAttributeGroupTypeWithName(const char* pName, CAttributeGroup *pAttributeGroup)
{
    assert (pAttributeGroup != nullptr);
    if (pName == nullptr || pAttributeGroup == nullptr)
        return;

    if (m_pAttributeGroupTypePtrsMap->getValue(pName) != nullptr)
    {
        m_pAttributeGroupTypePtrsMap->remove(pName);
        //throw MakeExceptionFromMap(EX_STR_ATTRIBUTE_GROUP_ALREADY_DEFINED);
    }

    assert(pAttributeGroup->getLinkCount() == 1);
    m_pAttributeGroupTypePtrsMap->setValue(pName, pAttributeGroup);
}

CAttributeGroup* CSchemaMapManager::getAttributeGroupFromXPath(const char *pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    if (pXPath == nullptr || *pXPath == 0)
        return nullptr;

    CAttributeGroup **ppAttributeGroup = m_pAttributeGroupTypePtrsMap->getValue(pXPath);
    assert(ppAttributeGroup != nullptr);

    if (ppAttributeGroup != nullptr)
        return *ppAttributeGroup;
    else
        return nullptr;
}

CElement* CSchemaMapManager::getElementWithName(const char* pName)
{
    assert (pName != nullptr && *pName != 0);
    if (pName != nullptr && *pName != 0)
    {
        CElement **ppElement = m_pElementNamePtrsMap->getValue(pName);
        assert(ppElement != nullptr);

        if (ppElement != nullptr)
            return *ppElement;
        else
            return nullptr;
    }
    else
        return nullptr;
}

void CSchemaMapManager::setElementWithName(const char* pName, CElement *pElement)
{
    assert (pName != nullptr && *pName != 0 && pElement != nullptr);

    if (pName != nullptr && *pName != 0 && pElement != nullptr)
    {
        assert (pElement != nullptr);
        if (pName == nullptr || *pName == 0 || pElement == nullptr)
            return;
        if (m_pElementNamePtrsMap->getValue(pName) != nullptr)
        {
            if (STRICTNESS_LEVEL >= DEFAULT_STRICTNESS)
                assert(!"Redefintion");
            else
                PROGLOG("Symbol redefinition.  Possible misprocessing xsd file. Ignoring..");
        }
        assert(m_pElementNamePtrsMap->getLinkCount() == 1);
        m_pElementNamePtrsMap->setValue(pName, pElement);
    }
}

void CSchemaMapManager::addMapOfXPathToAttribute(const char*pXPath, CAttribute *pAttribute)
{
    assert (pAttribute != nullptr);
    assert(pXPath != nullptr && *pXPath != 0);

    // TODO:: throw exception if problems here
    CAttribute **ppAttribute = m_pAttributePtrsMap->getValue(pXPath);

    if (ppAttribute != nullptr && *ppAttribute != pAttribute)
        assert(!"Assigning different node with same xpath! delete it first!");

    // should I remove automatically?
    assert(pAttribute->getLinkCount() == 1);

    StringBuffer strXPath(pXPath);
    strXPath.replace('/','_');
    m_pAttributePtrsMap->setValue(pXPath, pAttribute);
    m_pAttributePtrsMap->setValue(strXPath.str(), pAttribute);
}

void CSchemaMapManager::removeMapOfXPathToAttribute(const char*pXPath)
{
    assert (m_pAttributePtrsMap->find(pXPath) != nullptr);

    StringBuffer strXPath(pXPath);
    strXPath.replace('/','_');

    m_pAttributePtrsMap->remove(pXPath);
    m_pAttributePtrsMap->remove(strXPath.str());
}

CAttribute* CSchemaMapManager::getAttributeFromXPath(const char* pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    CAttribute **pAttribute = m_pAttributePtrsMap->getValue(pXPath);

    if (pAttribute == nullptr)
    {
        StringBuffer strXPath(pXPath);
        strXPath.replace('/','_');
        pAttribute = m_pAttributePtrsMap->getValue(pXPath);
    }

    if (STRICTNESS_LEVEL >= DEFAULT_STRICTNESS)
        assert(pAttribute != nullptr);
    if (pAttribute == nullptr)
        return nullptr;

    return *pAttribute;
}

void CSchemaMapManager::addMapOfXSDXPathToElementArray(const char*pXPath, CElementArray *pElementArray)
{
    assert (pElementArray != nullptr);
    assert(pXPath != nullptr && *pXPath != 0);
    assert(pElementArray->getLinkCount() == 1);

    if (m_pElementArrayPtrsMap->find(pXPath) != nullptr)
        return;  // already mapped, we must be dealing with live data

    PROGLOG("Mapping XSD XPath %s to %p elementarray", pXPath, pElementArray);
    m_pElementArrayPtrsMap->setValue(pXPath, pElementArray);
}

void CSchemaMapManager::removeMapOfXSDXPathToElementArray(const char*pXPath)
{
    assert (m_pElementArrayPtrsMap->find(pXPath) != nullptr);
    m_pElementArrayPtrsMap->remove(pXPath);
}

CElementArray* CSchemaMapManager::getElementArrayFromXSDXPath(const char* pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    if (pXPath == nullptr)
        return nullptr;

    CElementArray** ppElementArray = m_pElementArrayPtrsMap->getValue(pXPath);
    if (ppElementArray != nullptr)
        return *ppElementArray;
    else
        return nullptr;
}

void CSchemaMapManager::addMapOfXPathToElement(const char* pXPath, CElement *pElement,  bool bIsTopLevelElement)
{
    assert (pElement != nullptr);
    assert(pXPath != nullptr && *pXPath != 0);
    PROGLOG("Mapping XPath %s to %p element", pXPath, pElement);
    assert(pElement->getLinkCount() == 1);

    assert(m_pElementPtrsMap->getValue(pXPath) == nullptr);
    m_pElementPtrsMap->setValue(pXPath, pElement);
}

void CSchemaMapManager::removeMapOfXPathToElement(const char*pXPath)
{
    assert (m_pElementPtrsMap->find(pXPath) != nullptr);
    m_pElementPtrsMap->remove(pXPath);
}

CElement* CSchemaMapManager::getElementFromXPath(const char *pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    CElement **ppElement = m_pElementPtrsMap->getValue(pXPath);

    assert(ppElement != nullptr);
    if (ppElement != nullptr)
        return *ppElement;
    else
        return nullptr;
}

void CSchemaMapManager::addMapOfXSDXPathToElement(const char* pXPath, CElement *pElement)
{
    assert (pElement != nullptr);
    assert(pXPath != nullptr && *pXPath != 0);

    if (pElement != nullptr && pXPath != nullptr && *pXPath != 0)
    {
        StringBuffer strFullXPath;
        strFullXPath.appendf("%s-%s",pElement->getConstSchemaNode()->getXSDXPath(), pXPath);
        m_pXSDToElementPtrsMap->setValue(strFullXPath.str(), pElement);
    }
}

void CSchemaMapManager::addMapOfXPathToElementArray(const char* pXPath, CElementArray *pElementArray)
{
    assert(pXPath != nullptr && *pXPath != 0);
    assert(pElementArray);

    PROGLOG("Mapping Env XML XPath %s to %p elementarray", pXPath, pElementArray);

    if (pElementArray != nullptr && pXPath != nullptr && *pXPath != 0)
    {
        StringBuffer strXPath(pXPath);
        stripTrailingIndex(strXPath);

        this->m_pElementArrayPtrsMap->setValue(strXPath.str(), pElementArray);
    }
}

void CSchemaMapManager::removeMapOfXPathToElementArray(const char* pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    assert(m_pElementArrayPtrsMap->find(pXPath) != nullptr);

    this-m_pElementArrayPtrsMap->remove(pXPath);
}

CElementArray* CSchemaMapManager::getElementArrayFromXPath(const char *pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    assert(m_pElementArrayPtrsMap->getValue(pXPath) != nullptr);

    return *(this->m_pElementArrayPtrsMap->getValue(pXPath));
}

CElement* CSchemaMapManager::getElementFromXSDXPath(const char *pXPath) const
{
    UNIMPLEMENTED;
    //return nullptr;
}

void CSchemaMapManager::addMapOfXSDXPathToKey(const char* pXPath, CKey *pKey)
{
    assert (pKey != nullptr);
    assert (pXPath != nullptr && *pXPath != 0);

    if (pKey != nullptr && pXPath != nullptr && *pXPath != 0)
        m_pStringToKeyPtrsMap->setValue(pXPath, pKey);
}

CKey* CSchemaMapManager::getKeyFromXSDXPath(const char *pXPath) const
{
    assert(pXPath != nullptr && *pXPath != 0);
    if (pXPath != nullptr && *pXPath != 0)
    {
        CKey **ppKey = m_pStringToKeyPtrsMap->getValue(pXPath);
        assert(ppKey != nullptr);

        if (ppKey != nullptr)
            return *ppKey;
        else
            return nullptr;
    }
    assert(!"Control should reach here.  xpath invalid?");
    return nullptr;
}

void CSchemaMapManager::addMapOfXPathToRestriction(const char*pXPath, CRestriction *pRestriction)
{
    assert (pRestriction != nullptr);
    assert(pXPath != nullptr && *pXPath != 0);
    assert(m_pRestrictionPtrsMap->find(pXPath) == nullptr);
    assert(pRestriction->getLinkCount() == 1);

    m_pRestrictionPtrsMap->setValue(pXPath, pRestriction);
}

void CSchemaMapManager::removeMapOfXPathToRestriction(const char*pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);
    m_pRestrictionPtrsMap->remove(pXPath);
}

CRestriction* CSchemaMapManager::getRestrictionFromXPath(const char* pXPath)
{
    assert(pXPath != nullptr && *pXPath != 0);

    CRestriction **ppRestriction = m_pRestrictionPtrsMap->getValue(pXPath);
    assert(ppRestriction != nullptr);

    if (ppRestriction != nullptr)
        return *ppRestriction;
    else
        return nullptr;
}

int CSchemaMapManager::getNumberOfComponents() const
{
    int nCount = 0;
    HashIterator iter(*(m_pElementPtrsMap.get()));

    ForEach(iter)
    {
        CElement *pElement = *(m_pElementPtrsMap->mapToValue(&iter.query()));
        if (pElement->isTopLevelElement() == true)
            nCount++;
    }
    return nCount;
}

CElement* CSchemaMapManager::getComponent(int index)
{
    assert(index >= 0 && index < getNumberOfComponents());
    HashIterator iter(*(m_pElementPtrsMap.get()));

    int nCount = 0;

    ForEach(iter)
    {
        CElement *pElement = *(m_pElementPtrsMap->mapToValue(&iter.query()));
        if (pElement->isTopLevelElement() == true)
        {
            if (nCount == index)
                return pElement;
            nCount++;
        }        
    }
    return nullptr;
}

int CSchemaMapManager::getIndexOfElement(const CElement *pElem)
{
    int nCount = 0;
    HashIterator iter(*(m_pElementPtrsMap.get()));

    ForEach(iter)
    {
        CElement *pElement = *(m_pElementPtrsMap->mapToValue(&iter.query()));

        if (pElement == pElem)
            return nCount;
        if (pElement->isTopLevelElement() == true)
            nCount++;
    }
    assert(false);
    return -1;
}

enum NODE_TYPES CSchemaMapManager::getEnumFromTypeName(const char *pTypeName) const
{
    if (pTypeName == nullptr || *pTypeName == 0)
        return XSD_ERROR;

    enum NODE_TYPES *eRet = (m_pStringToEnumMap->getValue(pTypeName));

    if (eRet == nullptr || *eRet == XSD_ERROR)
    {
        if (STRICTNESS_LEVEL >= MAXIMUM_STRICTNESS)
            assert(!"Unknown XSD built in data type");

        PROGLOG("Unknown XSD built in data type");
        return XSD_ERROR;
    }
    return *eRet;
}

const char* CSchemaMapManager::getTypeNameFromEnum(enum NODE_TYPES eType, bool bForDump) const
{
    if (eType-XSD_DT_NORMALIZED_STRING > 0 && eType-XSD_DT_NORMALIZED_STRING < XSD_ERROR)
        return m_enumArray[eType-XSD_DT_NORMALIZED_STRING][bForDump ? 1 : 0];

    assert(!"Unknown XSD built-in type");
    PROGLOG("Unknown XSD built-in type");
    return nullptr;
}


bool CSchemaMapManager::isNodeExcluded(enum NODE_TYPES nodeType) const
{
    auto result = m_setOfExludedNodeTypes.find(nodeType);

    if (result == m_setOfExludedNodeTypes.end())
        return true;

    return false;
}

void CSchemaMapManager::addExludedNode(enum NODE_TYPES nodeType)
{
    m_setOfExludedNodeTypes.insert(nodeType);
}
