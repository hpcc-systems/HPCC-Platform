#ifndef _SCHEMA_PATTERN_HPP_
#define _SCHEMA_PATTERN_HPP_

#include "SchemaCommon.hpp"

class CPattern : public CXSDNodeWithRestrictions<CPattern>
{
    friend class CXSDNodeWithRestrictions<CPattern>;
public:

    virtual ~CPattern()
    {
    }

    static CPattern* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(Pattern)

private:

    CPattern(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CPattern>::CXSDNodeWithRestrictions(pParentNode, XSD_PATTERN)
    {
    }


};

#endif // _SCHEMA_PATTERN_HPP_
