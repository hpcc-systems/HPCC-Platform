#ifndef _SCHEMA_MAX_EXCLUSIVE_HPP_
#define _SCHEMA_MAX_EXCLUSIVE_HPP_

#include "SchemaCommon.hpp"

class CMaxExclusive : public CXSDNodeWithRestrictions<CMaxExclusive>
{
    friend class CXSDNodeWithRestrictions<CMaxExclusive>;

public:

    virtual ~CMaxExclusive()
    {
    }

    static CMaxExclusive* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CMaxExclusive(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMaxExclusive>::CXSDNodeWithRestrictions(pParentNode, XSD_MAX_EXCLUSIVE), m_nMaxExclusive(-1)
    {
    }

    GETTERSETTER(Value)
    GETTERSETTERINT(MaxExclusive)
};

#endif // _SCHEMA_MAX_EXCLUSIVE_HPP_
