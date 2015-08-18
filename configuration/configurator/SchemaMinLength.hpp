#ifndef _SCHEMA_MIN_LENGTH_HPP_
#define _SCHEMA_MIN_LENGTH_HPP_

#include "SchemaCommon.hpp"

class CMinLength : public CXSDNodeWithRestrictions<CMinLength>
{
    friend class CXSDNodeWithRestrictions<CMinLength>;
public:

    virtual ~CMinLength()
    {
    }

    static CMinLength* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MinLength)

protected:

    CMinLength(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMinLength>::CXSDNodeWithRestrictions(pParentNode, XSD_MIN_LENGTH), m_nMinLength(-1)
    {
    }
};

#endif // _SCHEMA_MIN_LENGTH_HPP_
