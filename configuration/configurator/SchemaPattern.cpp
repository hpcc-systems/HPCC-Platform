#include "SchemaPattern.hpp"

CPattern* CPattern::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    CPattern *pPattern = CXSDNodeWithRestrictions<CPattern>::load(pParentNode, pSchemaRoot, xpath);

    if (pPattern == NULL)
    {
        return NULL;
    }

    pPattern->setPattern(pPattern->getValue());

    return pPattern;
}
