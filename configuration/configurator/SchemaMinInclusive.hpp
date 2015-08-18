#ifndef _SCHEMA_MIN_INCLUSIVE_HPP_
#define _SCHEMA_MIN_INCLUSIVE_HPP_

#include "SchemaCommon.hpp"

class CMinInclusive : public CXSDNodeWithRestrictions<CMinInclusive>
{
    friend class CXSDNodeWithRestrictions<CMinInclusive>;
public:

    virtual ~CMinInclusive()
    {
    }

    static CMinInclusive* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MinInclusive)

protected:

    CMinInclusive(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMinInclusive>::CXSDNodeWithRestrictions(pParentNode, XSD_MIN_INCLUSIVE), m_nMinInclusive(-1)
    {
    }

};

#endif // _SCHEMA_MIN_INCLUSIVE_HPP_
