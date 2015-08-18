#include "SchemaMaxInclusive.hpp"

CMaxInclusive* CMaxInclusive::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    CMaxInclusive *pMaxInclusive = CXSDNodeWithRestrictions<CMaxInclusive>::load(pParentNode, pSchemaRoot, xpath);

    if (pMaxInclusive == NULL)
    {
        return NULL;
    }

    pMaxInclusive->setMaxInclusive(pMaxInclusive->getValue());

    return pMaxInclusive;
}
