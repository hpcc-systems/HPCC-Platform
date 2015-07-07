#include "SchemaMaxExclusive.hpp"

CMaxExclusive* CMaxExclusive::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    CMaxExclusive *pMaxExclusive = CXSDNodeWithRestrictions<CMaxExclusive>::load(pParentNode, pSchemaRoot, xpath);

    if (pMaxExclusive == NULL)
    {
        return NULL;
    }

    pMaxExclusive->setMaxExclusive(pMaxExclusive->getValue());

    return pMaxExclusive;
}
