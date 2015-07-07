#include "SchemaUnique.hpp"

CUnique* CUnique::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode != NULL);
    assert(pParentNode->getNodeType() == XSD_UNIQUE_ARRAY);

    if (pSchemaRoot == NULL || pParentNode == NULL)
    {
        // TODO: Throw Exception
        return NULL;
    }

    CUnique *pUnique = NULL;

    if (xpath != NULL && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            return NULL;
        }

        const char* pName = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_NAME);

        assert(pName != NULL && *pName != 0);

        if (pName == NULL || *pName == 0)
        {
            assert(!"Throw Exception name can not be empty");
            // TODO: throw exception
        }
        else
        {
            pUnique = new CUnique(pParentNode);
            pUnique->setXSDXPath(xpath);
        }

        const char *pID = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_ID);

        if (pID != NULL)
        {
            pUnique->setID(pID);
        }
   }

    return pUnique;
}

void CUnique::dump(std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_UNIQUE_STR, offset);

    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, Name, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    QuickOutFooter(cout, XSD_UNIQUE_STR, offset);
}

CUniqueArray* CUniqueArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL || xpath == NULL)
    {
        return NULL;
    }

    StringBuffer strXPathExt(xpath);

    CUniqueArray *pUniqueArray = new CUniqueArray(pParentNode);

    pUniqueArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.clear().append(xpath).appendf("[%d]",count);

        CUnique *pUnique = CUnique::load(pUniqueArray, pSchemaRoot, strXPathExt.str());

        if (pUnique != NULL)
        {
            pUniqueArray->append(*pUnique);
        }

        count++;
    }

    if (pUniqueArray->length() == 0)
    {
        delete pUniqueArray;
        pUniqueArray = NULL;
    }

    return pUniqueArray;
}

void CUniqueArray::dump(std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_UNIQUE_ARRAY_STR, offset);

    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);

    QuickOutFooter(cout, XSD_UNIQUE_ARRAY_STR, offset);
}


