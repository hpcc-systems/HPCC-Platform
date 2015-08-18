#include "SchemaTotalDigits.hpp"

CTotalDigits* CTotalDigits::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    CTotalDigits *pTotalDigits = CXSDNodeWithRestrictions<CTotalDigits>::load(pParentNode, pSchemaRoot, xpath);

    if (pTotalDigits == NULL)
    {
        return NULL;
    }

    pTotalDigits->setTotalDigits(pTotalDigits->getValue());

    return pTotalDigits;
}
