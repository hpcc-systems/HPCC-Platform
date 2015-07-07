#include "SchemaCommon.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaEnumeration.hpp"
#include "SchemaRestriction.hpp"
#include "XMLTags.h"
#include "jptree.hpp"
#include "DocumentationMarkup.hpp"
#include "DojoJSMarkup.hpp"
#include "QMLMarkup.hpp"

CEnumeration* CEnumeration::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    CEnumeration *pEnumeration = new CEnumeration(pParentNode);

    pEnumeration->setXSDXPath(xpath);

    if (xpath && *xpath)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            return pEnumeration;
        }

        const char* pValue = pTree->queryProp(XML_ATTR_VALUE);

        if (pValue != NULL)
        {
            pEnumeration->setValue(pValue);
        }
    }

    return pEnumeration;
}

void CEnumeration::dump(std::ostream &cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ENUMERATION_STR, offset);

    QUICK_OUT(cout, Value, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, EnvValueFromXML,  offset);

    QuickOutFooter(cout, XSD_ENUMERATION_STR, offset);
}

void CEnumeration::getDocumentation(StringBuffer &strDoc) const
{
    strDoc.appendf("* %s %s\n", this->getValue(), DM_LINE_BREAK);
}

void CEnumeration::getDojoJS(StringBuffer &strJS) const
{
    strJS.appendf("%s%s%s%s%s%s",DJ_MEMORY_ENTRY_NAME_BEGIN, this->getValue(), DJ_MEMORY_ENTRY_NAME_END, DJ_MEMORY_ENTRY_ID_BEGIN, this->getValue(), DJ_MEMORY_ENTRY_ID_END);
}

void CEnumeration::getQML(StringBuffer &strQML, int idx) const
{
    strQML.append(QML_LIST_ELEMENT_BEGIN).append(this->getValue()).append(QML_LIST_ELEMENT_END);
    DEBUG_MARK_QML;
}

const char* CEnumeration::getXML(const char* /*pComponent*/)
{
    assert(!"Not Implemented");
    return NULL;
}

void CEnumeration::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(this->getValue() != NULL);

    const CAttribute *pAttribute = dynamic_cast<const CAttribute*>(this->getParentNodeByType(XSD_ATTRIBUTE));

    assert(pAttribute != NULL);

    this->setEnvXPath(strXPath.str());
}

void CEnumeration::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(this->getEnvXPath() != NULL);

    const CAttribute *pAttribute = dynamic_cast<const CAttribute*>(this->getParentNodeByType(XSD_ATTRIBUTE) );

    assert(pAttribute != NULL);

    StringBuffer strXPath(this->getEnvXPath());

    strXPath.append("[@").append(pAttribute->getName()).append("=\"").append(this->getValue()).append("\"]");

    if (pEnvTree->hasProp(strXPath.str()) == true)
    {
        this->setInstanceValueValid(true);
    }
    else
    {
        this->setInstanceValueValid(false);
    }
}

void CEnumerationArray::dump(std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ENUMERATION_ARRAY_STR, offset);

    QUICK_OUT_ARRAY(cout, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);

    QuickOutFooter(cout, XSD_ENUMERATION_ARRAY_STR, offset);
}

void CEnumerationArray::getDocumentation(StringBuffer &strDoc) const
{
    strDoc.append("\nChoices are: \n").append(DM_LINE_BREAK);
    QUICK_DOC_ARRAY(strDoc);
}

void CEnumerationArray::getDojoJS(StringBuffer &strJS) const
{
    QUICK_DOJO_JS_ARRAY(strJS);
}

void CEnumerationArray::getQML(StringBuffer &strQML, int idx) const
{
    QUICK_QML_ARRAY(strQML);
}

const char* CEnumerationArray::getXML(const char* /*pComponent*/)
{
    assert(false); // NOT IMPLEMENTED
    return NULL;
}

void CEnumerationArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(index == 1);  // Only 1 array of elements per node

    QUICK_ENV_XPATH(strXPath)

    this->setEnvXPath(strXPath);
}

void CEnumerationArray::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);

    if (pEnvTree->hasProp(this->getEnvXPath()) == false)
    {
        throw MakeExceptionFromMap(EX_STR_XPATH_DOES_NOT_EXIST_IN_TREE);
    }
    else
    {
        QUICK_LOAD_ENV_XML(pEnvTree)
    }

}

int CEnumerationArray::getEnvValueNodeIndex() const
{
    int len = this->length();

    for (int idx = 0; idx < len; idx++)
    {
        if (this->item(idx).isInstanceValueValid() == true)
        {
            return idx;
        }
    }

    //assert(false); // should never get here?

    return 1;
}

void CEnumerationArray::setEnvValueNodeIndex(int index)
{
    assert(index >= 0);
    assert(index < this->length());

    for (int idx = 0; idx < this->length(); idx++)
    {
        if (this->item(idx).isInstanceValueValid() == true)
        {
            this->item(idx).setInstanceValueValid(false);
        }
    }
    this->item(index).setInstanceValueValid(true);
}

CEnumerationArray* CEnumerationArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    CEnumerationArray *pEnumerationArray = new CEnumerationArray(pParentNode);

    pEnumerationArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> elemIter = pSchemaRoot->getElements(xpath);

    int count = 1;

    ForEach(*elemIter)
    {
        StringBuffer strXPathExt(xpath);
        strXPathExt.appendf("[%d]", count);

        CEnumeration *pEnumeration = CEnumeration::load(pEnumerationArray, pSchemaRoot, strXPathExt.str());

        pEnumerationArray->append(*pEnumeration);

        count++;
    }

    if (pEnumerationArray->length() == 0)
    {
        delete pEnumerationArray;
        return NULL;
    }

    SETPARENTNODE(pEnumerationArray, pParentNode);


    // No need to create a blank entry in the docs
    /*const CAttribute *pAttribute = dynamic_cast<const CAttribute*>(pEnumerationArray->getParentNodeByType(XSD_ATTRIBUTE));

    if (pAttribute != NULL)
    {
        if (pAttribute->getUse() != NULL && stricmp(pAttribute->getUse(),TAG_REQUIRED) != 0)
        {
            StringBuffer strXPathExt(xpath);

            strXPathExt.append("[value=\"\"]");

            CEnumeration *pEnumeration = CEnumeration::load(pEnumerationArray, pSchemaRoot, strXPathExt.str());

            pEnumerationArray->append(*pEnumeration);
        }
    }*/

    return pEnumerationArray;
}
