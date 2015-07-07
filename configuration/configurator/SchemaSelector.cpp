#include "SchemaSelector.hpp"
#include "SchemaCommon.hpp"

CSelector* CSelector::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode != NULL);
    assert(pParentNode->getNodeType() == XSD_KEY || pParentNode->getNodeType() == XSD_KEYREF || pParentNode->getNodeType() == XSD_UNIQUE);

    if (pSchemaRoot == NULL || pParentNode == NULL)
    {
        // TODO: Throw Exception
        assert(false);
        return NULL;
    }

    CSelector *pSelector = NULL;

    if (xpath != NULL && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            assert(!"Selector reuqired");
            // TODO: throw MakeExceptionFromMap("EX_STR_MISSING_SELECTOR_MISSING");
        }

         const char* pXPath = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_XPATH);

         assert(pXPath != NULL && *pXPath != 0);

         if (pXPath == NULL || *pXPath == 0)
         {
             assert(!"Throw Exception");
             return NULL;
             // TODO: throw exception
         }

         if (pXPath != NULL)
         {
             pSelector = new CSelector(pParentNode);
             pSelector->setXSDXPath(xpath);
             pSelector->setXPath(pXPath);
         }
         else
         {
             assert(!"selector can not be be empty!");
             // TODO: throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
         }

         const char *pID = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_ID);

         if (pID != NULL)
         {
             pSelector->setID(pID);
         }
    }

    return pSelector;
}

void CSelector::dump(std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SELECTOR_STR, offset);

    QUICK_OUT(cout, XPath, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    QuickOutFooter(cout, XSD_SELECTOR_STR, offset);
}
