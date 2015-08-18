#include <cassert>
#include "jptree.hpp"
#include "XMLTags.h"
#include "SchemaSequence.hpp"
#include "SchemaElement.hpp"
#include "QMLMarkup.hpp"


const CXSDNodeBase* CSequence::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
    {
        return this;
    }

    if (m_pElementArray != NULL)
    {
        pMatchingNode = m_pElementArray->getNodeByTypeAndNameDescending(eNodeType, pName);
    }

    return pMatchingNode;
}

CSequence* CSequence::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    CSequence *pSequence = NULL;

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    if (pSchemaRoot->queryPropTree(xpath) == NULL)
    {
        return NULL;  // no sequence node
    }

    StringBuffer strXPath(xpath);
    strXPath.append("/").append(XSD_TAG_ELEMENT);

    CElementArray *pElemArray = CElementArray::load(NULL, pSchemaRoot, strXPath.str());

    //PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    //PROGLOG("pElementArray = %p", pElemArray);

    if (pElemArray != NULL)
    {
        pSequence = new CSequence(pParentNode, pElemArray);

        pSequence->setXSDXPath(xpath);
    }

    SETPARENTNODE(pElemArray, pSequence)

    return pSequence;
}

void CSequence::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SEQUENCE_STR, offset);

    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pElementArray != NULL)
    {
        m_pElementArray->dump(cout, offset);
    }

    QuickOutFooter(cout, XSD_SEQUENCE_STR, offset);
}

void CSequence::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getDocumentation(strDoc);
    }
}

void CSequence::getDojoJS(StringBuffer &strJS) const
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getDojoJS(strJS);
    }
}

void CSequence::getQML(StringBuffer &strQML, int idx) const
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getQML(strQML);
    }
}

void CSequence::getQML2(StringBuffer &strQML, int idx) const
{
    if (m_pElementArray != NULL)
    {
        this->setUIType(this->getParentNode()->getUIType());
        m_pElementArray->getQML2(strQML);
    }
}

void CSequence::getQML3(StringBuffer &strQML, int idx) const
{   
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getQML3(strQML);
    }
}

void CSequence::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->populateEnvXPath(strXPath);
    }

    this->setEnvXPath(strXPath);
}

void CSequence::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->loadXMLFromEnvXml(pEnvTree);
    }
}

bool CSequence::hasChildElements() const
{
    if (this->m_pElementArray->length() > 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}
