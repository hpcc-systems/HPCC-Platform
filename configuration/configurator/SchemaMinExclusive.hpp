#ifndef _SCHEMA_MIN_EXCLUSIVE_HPP_
#define _SCHEMA_MIN_EXCLUSIVE_HPP_

#include "SchemaCommon.hpp"

class CMinExclusive : public CXSDNodeWithRestrictions<CMinExclusive>
{
    friend class CXSDNodeWithRestrictions<CMinExclusive>;
public:

    virtual ~CMinExclusive()
    {
    }

    static CMinExclusive* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MinExclusive)

protected:

    CMinExclusive(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMinExclusive>::CXSDNodeWithRestrictions<CMinExclusive>(pParentNode, XSD_MIN_EXCLUSIVE), m_nMinExclusive(-1)
    {
    }
};

#endif // _SCHEMA_MIN_EXCLUSIVE_HPP_
