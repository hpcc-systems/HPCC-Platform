#include "SchemaSimpleContent.hpp"
#include "SchemaAnnotation.hpp"
#include "SchemaExtension.hpp"
#include "SchemaRestriction.hpp"

CSimpleContent* CSimpleContent::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    if (pParentNode == NULL || pSchemaRoot == NULL)
    {
        return NULL;
    }

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);

    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_EXTENSION);

    CExtension *pExtension = NULL;
    CAnnotation *pAnnotation = NULL;
    CRestriction *pRestriction = NULL;

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) != NULL)
    {
        pExtension = CExtension::load(NULL, pSchemaRoot, strXPathExt.str());

        if (pExtension != NULL)
        {
            pExtension->initExtension();
        }
    }

    strXPathExt.set(xpath);
    strXPathExt.append("/").append(XSD_TAG_ANNOTATION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) != NULL)
    {
        pAnnotation = CAnnotation::load(NULL, pSchemaRoot, strXPathExt.str());
    }

    strXPathExt.set(xpath);
    strXPathExt.append("/").append(XSD_TAG_RESTRICTION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) != NULL)
    {
        pRestriction = CRestriction::load(NULL, pSchemaRoot, strXPathExt.str());
    }

    const char* pID =  NULL;
    pID = pTree->queryProp(XML_ATTR_ID);

    CSimpleContent *pSimpleContent = new CSimpleContent(pParentNode, pID);

    SETPARENTNODE(pExtension, pSimpleContent);
    SETPARENTNODE(pAnnotation, pSimpleContent);
    SETPARENTNODE(pRestriction, pSimpleContent);

    return pSimpleContent;
}


CSimpleContent::~CSimpleContent()
{
    delete m_pRestriction;
    delete m_pAnnotation;
    delete m_pExtension;
}

bool CSimpleContent::checkConstraint(const char *pValue) const
{
    return true;
}
