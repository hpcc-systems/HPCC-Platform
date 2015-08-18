#include "jptree.hpp"
#include "XMLTags.h"
#include "SchemaComplexContent.hpp"
#include "SchemaExtension.hpp"


CComplexContent* CComplexContent::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot);

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_EXTENSION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) == NULL)
    {
        return NULL;
    }

    CExtension* pExtension = CExtension::load(NULL, pSchemaRoot, strXPathExt.str());

    CComplexContent *pComplexContent = new CComplexContent(pParentNode, pExtension);

    pComplexContent->setXSDXPath(xpath);

    assert(pExtension != NULL);
    assert(pComplexContent != NULL);

    if (pExtension != NULL && pComplexContent != NULL)
    {
        SETPARENTNODE(pExtension, pComplexContent);
        pExtension->initExtension();
    }


    return pComplexContent;
}

void CComplexContent::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_COMPLEX_CONTENT_STR, offset);

    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pExtension != NULL)
    {
        m_pExtension->dump(cout, offset);
    }

    QuickOutFooter(cout, XSD_COMPLEX_CONTENT_STR, offset);
}


void CComplexContent::getDocumentation(StringBuffer &strDoc) const
{
    return;
    /*
    if (m_pExtension != NULL)
    {
        m_pExtension->getDocumentation(strDoc);
    }*/
}

void CComplexContent::getDojoJS(StringBuffer &strJS) const
{

}

void CComplexContent::getQML(StringBuffer &strQML, int idx) const
{

}

/*void CComplexContent::traverseAndProcessNodes() const
{
    CXSDNodeBase::processEntryHandlers(this);

    if (m_pExtension != NULL)
    {
        m_pExtension->traverseAndProcessNodes();
    }

    CXSDNodeBase::processExitHandlers(this);
}*/

const char* CComplexContent::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0 && m_pExtension != NULL)
    {
        m_strXML.append(m_pExtension->getXML(NULL));
    }

    return m_strXML.str();
}
