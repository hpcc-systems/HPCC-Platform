#ifndef _SCHEMA_MAX_INCLUSIVE_HPP_
#define _SCHEMA_MAX_INCLUSIVE_HPP_

#include "SchemaCommon.hpp"

class CMaxInclusive : public CXSDNodeWithRestrictions<CMaxInclusive>
{
    friend class CXSDNodeWithRestrictions<CMaxInclusive>;

public:

    virtual ~CMaxInclusive()
    {
    }

    static CMaxInclusive* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MaxInclusive)

protected:

    CMaxInclusive(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMaxInclusive>::CXSDNodeWithRestrictions(pParentNode, XSD_MAX_INCLUSIVE), m_nMaxInclusive(-1)
    {
    }
};

#endif // _SCHEMA_MAX_INCLUSIVE_HPP_
