#ifndef _SCHEMA_SELECTOR_HPP_
#define _SCHEMA_SELECTOR_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CSelector : public CXSDNode
{
public:

    virtual ~CSelector()
    {
    }

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const
    {
        assert(!"Not Implemented");
    }

    virtual void getDojoJS(StringBuffer &strJS) const
    {
        assert(!"Not Implemented");
    }

    virtual void getQML(StringBuffer &strQML, int idx = -1) const
    {
        assert(!"Not Implemented");
    }

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1)
    {
        assert(!"Not Implemented");
    }

    static CSelector* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(ID)
    GETTERSETTER(XPath)

protected:

    CSelector(CXSDNodeBase* pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_SELECTOR)
    {
    }

};

#endif // _SCHEMA_SELECTOR_HPP_
