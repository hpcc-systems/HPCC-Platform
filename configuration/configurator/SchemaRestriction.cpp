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

#include "jptree.hpp"
#include "SchemaRestriction.hpp"
#include "SchemaEnumeration.hpp"
#include "SchemaAttributes.hpp"
#include "XMLTags.h"
#include "ConfigSchemaHelper.hpp"
#include "QMLMarkup.hpp"
#include "DocumentationMarkup.hpp"
#include "SchemaMapManager.hpp"
#include "SchemaFractionDigits.hpp"
#include "SchemaLength.hpp"
#include "SchemaTotalDigits.hpp"
#include "SchemaWhiteSpace.hpp"
#include "SchemaSimpleContent.hpp"

#define QUICK_LOAD_XSD_RESTRICTIONS(X, Y)       \
    strXPathExt.set(xpath);                     \
    strXPathExt.append("/").append(Y);          \
    C##X *p##X = C##X::load(pRestriction, pSchemaRoot, strXPathExt.str());    \
    if (p##X != NULL) pRestriction->set##X(p##X);
    //if (p##X != NULL) pRestriction->C##X(p##X);

CRestriction::~CRestriction()
{
    CConfigSchemaHelper::getInstance()->getSchemaMapManager()->removeMapOfXPathToRestriction(this->getEnvXPath());
}

void CRestriction::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_RESTRICTION_STR, offset);
    QUICK_OUT_2(Base);
    QUICK_OUT_2(ID);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, EnvValueFromXML,  offset);
    QUICK_OUT_3(EnumerationArray)
    QUICK_OUT_3(FractionDigits)
    QUICK_OUT_3(Length)
    QUICK_OUT_3(MaxExclusive)
    QUICK_OUT_3(MaxInclusive)
    QUICK_OUT_3(MinExclusive)
    QUICK_OUT_3(MinInclusive)
    QUICK_OUT_3(MaxLength)
    QUICK_OUT_3(MinLength)
    QUICK_OUT_3(Pattern)
    QUICK_OUT_3(TotalDigits)
    QUICK_OUT_3(WhiteSpace)
    QuickOutFooter(cout, XSD_RESTRICTION_STR, offset);
}

void CRestriction::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pEnumerationArray != NULL)
    {
        strDoc.appendf("<%s>",DM_PARA);
        DEBUG_MARK_STRDOC
        m_pEnumerationArray->getDocumentation(strDoc);
        strDoc.appendf("</%s>",DM_PARA);
        DEBUG_MARK_STRDOC
    }
}

void CRestriction::getQML(StringBuffer &strQML, int idx) const
{
    if (m_pEnumerationArray != NULL)
    {
        assert(this->getConstAncestorNode(3) != NULL && this->getConstAncestorNode(3)->getNodeType() == XSD_ATTRIBUTE);

        const CAttribute *pAttrib = dynamic_cast<const CAttribute*>(this->getConstAncestorNode(3));
        assert(pAttrib != NULL);

        strQML.append(QML_ROW_BEGIN);
        strQML.append(QML_RECTANGLE_DEFAULT_COLOR_SCHEME_1_BEGIN);
        DEBUG_MARK_QML;

        strQML.append(QML_TEXT_BEGIN_2).append("\"  ").append(pAttrib->getName()).append("\"").append(QML_TEXT_END_2);
        strQML.append(QML_RECTANGLE_LIGHT_STEEEL_BLUE_END);

        StringBuffer strComboBoxID("combobox");
        CQMLMarkupHelper::getRandomID(&strComboBoxID);

        strQML.append(QML_COMBO_BOX_BEGIN);
        strQML.append(strComboBoxID);
        DEBUG_MARK_QML;
        strQML.append(QML_LIST_MODEL_BEGIN);
        DEBUG_MARK_QML;
        m_pEnumerationArray->getQML(strQML);
        DEBUG_MARK_QML;
        strQML.append(QML_LIST_MODEL_END);
        strQML.append(QML_COMBO_BOX_CURRENT_INDEX).append(QML_APP_DATA_GET_INDEX_BEGIN).append(this->getEnvXPath()).append(QML_APP_DATA_GET_INDEX_END);
        DEBUG_MARK_QML;
        strQML.append(QML_ON_CURRENT_INDEX_CHANGED).append(QML_APP_DATA_SET_INDEX_BEGIN).append(this->getEnvXPath()).append("\", ").append(strComboBoxID).append(QML_APP_DATA_SET_INDEX_END);
        DEBUG_MARK_QML;
        strQML.append(QML_COMBO_BOX_END);
        strQML.append(QML_ROW_END);
        DEBUG_MARK_QML;
    }
}

void CRestriction::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);
    CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXPathToRestriction(this->getEnvXPath(), this);

    if (this->m_pEnumerationArray != NULL)
        this->m_pEnumerationArray->populateEnvXPath(strXPath);
}

void CRestriction::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    if (m_pEnumerationArray != NULL)
        m_pEnumerationArray->loadXMLFromEnvXml(pEnvTree);
}

CRestriction* CRestriction::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    if (pParentNode == NULL || pSchemaRoot == NULL)
        return NULL;

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);
    const char* pID =  NULL;
    const char* pBase =  NULL;

    pID = pTree->queryProp(XML_ATTR_ID);
    pBase = pTree->queryProp(XML_ATTR_BASE);

    CRestriction* pRestriction = new CRestriction(pParentNode, pID, pBase);
    pRestriction->setXSDXPath(xpath);

    StringBuffer strXPathExt;
    QUICK_LOAD_XSD_RESTRICTIONS(EnumerationArray, XSD_TAG_ENUMERATION)
    QUICK_LOAD_XSD_RESTRICTIONS(FractionDigits, XSD_TAG_FRACTION_DIGITS)
    QUICK_LOAD_XSD_RESTRICTIONS(Length, XSD_TAG_LENGTH)
    QUICK_LOAD_XSD_RESTRICTIONS(MaxExclusive, XSD_TAG_MAX_EXCLUSIVE)
    QUICK_LOAD_XSD_RESTRICTIONS(MaxInclusive, XSD_TAG_MAX_INCLUSIVE)
    QUICK_LOAD_XSD_RESTRICTIONS(MinExclusive, XSD_TAG_MIN_EXCLUSIVE)
    QUICK_LOAD_XSD_RESTRICTIONS(MinInclusive, XSD_TAG_MIN_INCLUSIVE)
    QUICK_LOAD_XSD_RESTRICTIONS(MaxLength, XSD_TAG_MAX_LENGTH)
    QUICK_LOAD_XSD_RESTRICTIONS(MinLength, XSD_TAG_MIN_LENGTH)
    QUICK_LOAD_XSD_RESTRICTIONS(Pattern, XSD_TAG_PATTERN)
    QUICK_LOAD_XSD_RESTRICTIONS(TotalDigits, XSD_TAG_TOTAL_DIGITS)
    QUICK_LOAD_XSD_RESTRICTIONS(WhiteSpace, XSD_TAG_WHITE_SPACE)

    if (pBase != NULL && *pBase != 0 && pRestriction != NULL)
        CConfigSchemaHelper::getInstance()->addNodeForBaseProcessing(pRestriction);

    return pRestriction;
}

const char* CRestriction::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length () == 0)
    {
        m_strXML.append("<").append(getBase()).append("\n");
        m_strXML.append("<").append(getID()).append("\n");
        m_strXML.append("/>\n");
    }
    return m_strXML.str();
}

bool CRestriction::checkConstraint(const char *pValue) const
{
    const CXSDNodeBase *pNodeBase = this->getBaseNode();
    assert(pNodeBase != NULL);

    if (pNodeBase != NULL)
    {
        const CXSDBuiltInDataType *pNodeBuiltInType = dynamic_cast<const CXSDBuiltInDataType*>(pNodeBase);

        if (pNodeBuiltInType != NULL && pNodeBuiltInType->checkConstraint(pValue) == false)
            return false;

        if (pNodeBase->getNodeType() == XSD_SIMPLE_TYPE)
        {
            const CSimpleType *pNodeSimpleType = dynamic_cast<const CSimpleType*>(pNodeBase);

            if (pNodeSimpleType != NULL && pNodeSimpleType->checkConstraint(pValue) == false)
               return false;
        }
        else if (pNodeBase->getNodeType() == XSD_SIMPLE_CONTENT)
        {
            const CSimpleContent *pNodeSimpleContent = dynamic_cast<const CSimpleContent*>(pNodeBase);

            if (pNodeSimpleContent != NULL && pNodeSimpleContent->checkConstraint(pValue) == false)
                return false;
        }
         assert(!"Unknown base node in restriction");
         return false;
    }
    return true;
}
