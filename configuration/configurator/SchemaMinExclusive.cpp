#include "SchemaMinExclusive.hpp"

CMinExclusive* CMinExclusive::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    CMinExclusive *pMinExclusive = CXSDNodeWithRestrictions<CMinExclusive>::load(pParentNode, pSchemaRoot, xpath);

    if (pMinExclusive == NULL)
    {
        return NULL;
    }

    pMinExclusive->setMinExclusive(pMinExclusive->getValue());

    return pMinExclusive;
}
