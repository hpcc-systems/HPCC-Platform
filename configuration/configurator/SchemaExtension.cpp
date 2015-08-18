#include "XMLTags.h"
#include "jptree.hpp"
#include "SchemaExtension.hpp"
#include "SchemaComplexType.hpp"
#include "SchemaSchema.hpp"
#include "ConfigSchemaHelper.hpp"

static const char* DEFAULT_ENVIRONMENT_XSD("Environment.xsd");

void CExtension::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_EXTENSION_STR, offset);

    QUICK_OUT(cout, Base, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (this->getBaseNode() != NULL)
    {
        this->getBaseNode()->dump(cout, offset);
    }

    QuickOutFooter(cout, XSD_EXTENSION_STR, offset);
}

/*void CExtension::traverseAndProcessNodes() const
{
    CXSDNodeBase::processEntryHandlers(this);

    if (this->getBaseNode() != NULL)
    {
        this->getBaseNode()->traverseAndProcessNodes();
    }

    CXSDNodeBase::processExitHandlers(this);
}*/

const char* CExtension::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0 && m_pXSDNode != NULL)
    {
        m_pXSDNode->getXML(NULL);
    }

    return NULL;
}

void CExtension::initExtension()
{
    NODE_TYPES eNodeType[] = { XSD_SIMPLE_TYPE, XSD_SIMPLE_CONTENT };

    const CXSDNodeBase *pBaseNode = (dynamic_cast<const CXSDNodeBase*>(this));

    if (pBaseNode != NULL)
    {
        pBaseNode->getNodeByTypeAndNameAscending( eNodeType, this->getBase());

       this->setBaseNode(const_cast<CXSDNodeBase*>(pBaseNode));
    }
}

CExtension* CExtension::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    CExtension *pExtension = NULL;

    if (xpath && *xpath)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            return NULL; // no xs:extension node
        }

        const char* pBase = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_BASE);

        if (pBase != NULL)
        {
            pExtension = new CExtension(pParentNode);
            pExtension->setXSDXPath(xpath);
            pExtension->setBase(pBase);

            /*CXSDNodeBase *pBaseNode = pExtension->getNodeByTypeAndNameAscending( XSD_SIMPLE_TYPE | XSD_COMPLEX_TYPE, pBase);

            assert(pBaseNode != NULL);  // temporary to catch built in types or not defined types

            if (pBaseNode != NULL)
            {
                pExtension->setBaseNode(pBaseNode);
            }*/
        }
    }

    if (pExtension != NULL)
    {
        CConfigSchemaHelper::getInstance()->addExtensionToBeProcessed(pExtension);
    }
    return pExtension;
}
