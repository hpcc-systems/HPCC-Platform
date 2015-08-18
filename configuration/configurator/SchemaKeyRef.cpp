#include "SchemaKeyRef.hpp"
#include "SchemaSelector.hpp"
#include "SchemaCommon.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaMapManager.hpp"

CKeyRef* CKeyRef::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode != NULL);
    assert(pParentNode->getNodeType() == XSD_KEYREF_ARRAY);

    if (pSchemaRoot == NULL || pParentNode == NULL)
    {
        // TODO: Throw Exception
        return NULL;
    }

    CKeyRef *pKeyRef = new CKeyRef(pParentNode);

    if (xpath != NULL && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            return NULL; // no xs:KeyRef
        }

        const char* pName = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_NAME);
        const char* pRefer = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_REFER);

         if (pName != NULL && pRefer != NULL)
         {
             pKeyRef = new CKeyRef(pParentNode);
             pKeyRef->setXSDXPath(xpath);
             pKeyRef->setName(pName);
         }
         else
         {
             assert(!"value attribute can be empty!");
             // TODO: throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
         }

         const char *pID = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_ID);

         if (pID != NULL)
         {
             pKeyRef->setID(pID);
         }

         StringBuffer strXPathExt(xpath);
         strXPathExt.append("/").append(XSD_TAG_FIELD);
         pKeyRef->m_pFieldArray = CFieldArray::load(pKeyRef, pSchemaRoot, strXPathExt.str());

         strXPathExt.clear().set(xpath);
         strXPathExt.append("/").append(XSD_TAG_SELECTOR);
         pKeyRef->m_pSelector = CSelector::load(pKeyRef, pSchemaRoot, strXPathExt.str());
    }

    return pKeyRef;
}


void CKeyRef::dump(std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_KEYREF_STR, offset);

    QUICK_OUT(cout, Name, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, Refer, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pFieldArray != NULL)
    {
        m_pFieldArray->dump(cout, offset);
    }

    if (m_pSelector != NULL)
    {
        m_pSelector->dump(cout, offset);
    }

    QuickOutFooter(cout, XSD_KEYREF_STR, offset);
}

bool CKeyRef::checkConstraint(const char *pValue) const
{
    assert (pValue != NULL);

    if (pValue == NULL)
    {
        return true;
    }
    else
    {
        StringBuffer strQName(this->getXSDXPath());
        strQName.append("/").append(this->getRefer());

        CKey *pKey = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getKeyFromXSDXPath(strQName.str());

        return pKey->checkConstraint(pValue);
    }
}


CKeyRefArray* CKeyRefArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode->getNodeType() == XSD_ELEMENT);

    if (pSchemaRoot == NULL || xpath == NULL)
    {
        return NULL;
    }

    StringBuffer strXPathExt(xpath);

    CKeyRefArray *pKeyRefArray = new CKeyRefArray(pParentNode);
    pKeyRefArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.clear().append(xpath).appendf("[%d]",count);

        CKeyRef *pKeyRef = CKeyRef::load(pKeyRefArray, pSchemaRoot, strXPathExt.str());

        if (pKeyRef != NULL)
        {
            pKeyRefArray->append(*pKeyRef);
        }

        count++;
    }

    if (pKeyRefArray->length() == 0)
    {
        delete pKeyRefArray;
        pKeyRefArray = NULL;
    }

    return pKeyRefArray;
}

void CKeyRefArray::dump(std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_KEYREF_ARRAY_STR, offset);

    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);

    QuickOutFooter(cout, XSD_KEYREF_ARRAY_STR, offset);
}
