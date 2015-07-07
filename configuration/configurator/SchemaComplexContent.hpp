#ifndef _SCHEMA_COMPLEX_CONTENT_HPP_
#define _SCHEMA_COMPLEX_CONTENT_HPP_

#include "SchemaCommon.hpp"

class IPropertyTree;
class CExtension;

class CComplexContent : public CXSDNode
{
public:

    virtual ~CComplexContent()
    {
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    virtual void getQML(StringBuffer &strQML, int idx = -1) const;

    //virtual void traverseAndProcessNodes() const;

    virtual const char* getXML(const char* /*pComponent*/);

    static CComplexContent* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CComplexContent(CXSDNodeBase* pParentNode, CExtension *pExtension = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_COMPLEX_CONTENT), m_pExtension(pExtension)
    {
    }

    CExtension *m_pExtension;

private:

    CComplexContent(CXSDNodeBase* pParentNode = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_COMPLEX_CONTENT)
    {
    }
};

#endif // _SCHEMA_COMPLEX_CONTENT_HPP_
