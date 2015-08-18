#ifndef _SCHEMA_WHITE_SPACE_HPP_
#define _SCHEMA_WHITE_SPACE_HPP_

#include "SchemaCommon.hpp"

class CWhiteSpace : public CXSDNodeWithRestrictions<CWhiteSpace>
{
    friend class CXSDNodeWithRestrictions<CWhiteSpace>;
public:

    virtual ~CWhiteSpace()
    {
    }

    static CWhiteSpace* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(Value)

protected:

    CWhiteSpace(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CWhiteSpace>::CXSDNodeWithRestrictions(pParentNode, XSD_WHITE_SPACE)
    {
    }

    int m_nWhiteSpace;

};

#endif // _SCHEMA_WHITE_SPACE_HPP_
