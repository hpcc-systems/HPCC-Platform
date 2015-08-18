#ifndef _SCHEMA_TOTAL_DIGITS_HPP_
#define _SCHEMA_TOTAL_DIGITS_HPP_

#include "SchemaCommon.hpp"

class CTotalDigits : public CXSDNodeWithRestrictions<CTotalDigits>
{
    friend class CXSDNodeWithRestrictions<CTotalDigits>;
public:

    virtual ~CTotalDigits()
    {
    }

    static CTotalDigits* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(TotalDigits)

protected:

    CTotalDigits(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CTotalDigits>::CXSDNodeWithRestrictions(pParentNode, XSD_TOTAL_DIGITS), m_nTotalDigits(-1)
    {
    }
};

#endif // _SCHEMA_TOTAL_DIGITS_HPP_
