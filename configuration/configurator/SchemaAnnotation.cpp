#include <cassert>
#include "XMLTags.h"
#include "jptree.hpp"
#include "SchemaAnnotation.hpp"
#include "SchemaDocumentation.hpp"
#include "SchemaAppInfo.hpp"

CAnnotation* CAnnotation::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    StringBuffer strXPathExt(xpath);

    strXPathExt.append("/").append(XSD_TAG_DOCUMENTATION);
    CDocumentation *pDocumentation = CDocumentation::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_APP_INFO);
    CAppInfo *pAnnInfo = CAppInfo::load(NULL, pSchemaRoot, strXPathExt.str());

    CAnnotation *pAnnotation = new CAnnotation(pParentNode, pDocumentation, pAnnInfo);

    pAnnotation->setXSDXPath(xpath);

    SETPARENTNODE(pDocumentation, pAnnotation);
    SETPARENTNODE(pAnnInfo, pAnnotation);

    return pAnnotation;
}

void CAnnotation::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ANNOTATION_STR, offset);

    QUICK_OUT(cout, XSDXPath, offset);

    if (m_pAppInfo != NULL)
    {
        m_pAppInfo->dump(cout, offset);
    }

    if (m_pDocumentation != NULL)
    {
        m_pDocumentation->dump(cout, offset);
    }

    QuickOutFooter(cout, XSD_ANNOTATION_STR, offset);
}

/*void CAnnotation::traverseAndProcessNodes() const
{
     CXSDNodeBase::processEntryHandlers(this);

     if (m_pAppInfo != NULL)
     {
         m_pAppInfo->traverseAndProcessNodes();
     }

     if (m_pDocumentation != NULL)
     {
         m_pDocumentation->traverseAndProcessNodes();
     }

     CXSDNodeBase::processExitHandlers(this);
}*/

void CAnnotation::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pAppInfo != NULL)
    {
        m_pAppInfo->getDocumentation(strDoc);
    }

    if (m_pDocumentation != NULL)
    {
        m_pDocumentation->getDocumentation(strDoc);
    }
}

void CAnnotation::getDojoJS(StringBuffer &strJS) const
{
    if (m_pAppInfo != NULL)
    {
        m_pAppInfo->getDojoJS(strJS);
    }

    if (m_pDocumentation != NULL)
    {
        m_pDocumentation->getDojoJS(strJS);
    }
}

void CAnnotation::getQML(StringBuffer &strQML, int idx) const
{
    if (m_pAppInfo != NULL)
    {
        m_pAppInfo->getQML(strQML);
    }

    if (m_pDocumentation != NULL)
    {
        m_pDocumentation->getQML(strQML);
    }
}

void CAnnotation::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    if (m_pAppInfo != NULL)
    {
        m_pAppInfo->loadXMLFromEnvXml(pEnvTree);
    }

    if (m_pDocumentation != NULL)
    {
        m_pDocumentation->loadXMLFromEnvXml(pEnvTree);
    }
}
