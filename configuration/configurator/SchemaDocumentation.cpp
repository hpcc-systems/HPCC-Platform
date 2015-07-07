#include "jptree.hpp"
#include "jstring.hpp"
#include "SchemaDocumentation.hpp"
#include "DocumentationMarkup.hpp"

void CDocumentation::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_DOCUMENTATION_STR, offset);

    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout,Documentation, offset);

    QuickOutFooter(cout, XSD_DOCUMENTATION_STR, offset);
}

/*void CDocumentation::traverseAndProcessNodes() const
{
    CXSDNodeBase::processEntryHandlers(this);
    CXSDNodeBase::processExitHandlers(this);
}*/

CDocumentation* CDocumentation::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    if (pSchemaRoot->queryPropTree(xpath) == NULL)
    {
        return NULL; // no documentation node
    }

    StringBuffer strDoc;

    if (xpath && *xpath)
    {
        strDoc.append(pSchemaRoot->queryPropTree(xpath)->queryProp(""));
    }

    CDocumentation *pDocumentation = new CDocumentation(pParentNode, strDoc.str());

    pDocumentation->setXSDXPath(xpath);

    return pDocumentation;
}

void  CDocumentation::getDocumentation(StringBuffer &strDoc) const
{
    strDoc.appendf("%s%s%s", DM_PARA_BEGIN, this->getDocString(), DM_PARA_END);
}

void CDocumentation::getDojoJS(StringBuffer &strJS) const
{

}
