#ifndef _SCHEMA_MAX_LENGTH_HPP_
#define _SCHEMA_MAX_LENGTH_HPP_

#include "SchemaCommon.hpp"

class CMaxLength : public CXSDNodeWithRestrictions<CMaxLength>
{
    friend class CXSDNodeWithRestrictions<CMaxLength>;
public:

    virtual ~CMaxLength()
    {
    }

    static CMaxLength* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MaxLength)

protected:

    CMaxLength(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMaxLength>::CXSDNodeWithRestrictions(pParentNode, XSD_MAX_LENGTH), m_nMaxLength(-1)
    {
    }
};

#endif // _SCHEMA_MAX_LENGTH_HPP_
