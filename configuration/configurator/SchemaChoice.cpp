#include "jptree.hpp"
#include "XMLTags.h"
#include "SchemaChoice.hpp"
#include "SchemaElement.hpp"
#include "DocumentationMarkup.hpp"


CChoice* CChoice::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL || xpath == NULL)
    {
        return NULL;
    }

    if (pSchemaRoot->queryPropTree(xpath) == NULL)
    {
        return NULL; // no xs:choice node found
    }

    StringBuffer strXPathExt(xpath);

    strXPathExt.append("/").append(XSD_TAG_ELEMENT);

    CElementArray *pElemArray = CElementArray::load(NULL, pSchemaRoot, strXPathExt.str());

    //PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    //PROGLOG("pElementArray = %p xpath=%s", pElemArray, xpath);

    assert(pElemArray);

    if (pElemArray == NULL)
    {
        return NULL;
    }

    CChoice *pChoice  = new CChoice(pParentNode, pElemArray);

    assert(pChoice);

    if (pChoice == NULL)
    {
        return NULL;
    }

    pChoice->setXSDXPath(xpath);

    SETPARENTNODE(pElemArray, pChoice);

    return pChoice;
}

const char* CChoice::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        if (m_pElementArray != NULL)
        {
            m_strXML.append(m_pElementArray->getXML(NULL));
        }
    }

    return m_strXML.str();
}

void CChoice::dump(std::ostream &cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_CHOICE_STR, offset);

    QUICK_OUT(cout, ID,         offset);
    QUICK_OUT(cout ,MinOccurs,  offset);
    QUICK_OUT(cout, MaxOccurs,  offset);
    QUICK_OUT(cout, XSDXPath,   offset);

    if (m_pElementArray != NULL)
    {
        m_pElementArray->dump(cout, offset);
    }

    QuickOutFooter(cout, XSD_CHOICE_STR, offset);
}

void CChoice::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getDocumentation(strDoc);
    }
}

void CChoice::getDojoJS(StringBuffer &strJS) const
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getDojoJS(strJS);
    }
}

void CChoice::getQML(StringBuffer &strQML, int idx) const
{
    if (m_pElementArray != NULL)
    {
        m_pElementArray->getQML(strQML);
    }
}


void CChoice::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);

    if (m_pElementArray != NULL)
    {
        m_pElementArray->populateEnvXPath(strXPath);
    }
}

void CChoice::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert (pEnvTree != NULL);

    if (m_pElementArray != NULL)
    {
        try
        {
            m_pElementArray->loadXMLFromEnvXml(pEnvTree);
        }
        catch (...)
        {

        }
    }
}
