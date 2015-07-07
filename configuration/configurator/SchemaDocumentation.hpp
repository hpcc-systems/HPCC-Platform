#ifndef _SCHEMA_DOCUMENATION_HPP_
#define _SCHEMA_DOCUMENATION_HPP_

#include "jiface.hpp"
#include "jstring.hpp"
#include "SchemaCommon.hpp"

class CDocumentation : public CXSDNode
{
public:

    virtual ~CDocumentation()
    {
    }

    GETTERSETTER(DocString)

    const char* getDocumentation() const
    {
        return m_strDocString.str();
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    //virtual void traverseAndProcessNodes() const;

    static CDocumentation* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CDocumentation(CXSDNodeBase* pParentNode, const char *pDocs = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_DOCUMENTATION), m_strDocString(pDocs)
    {
    }

private:

    CDocumentation() : CXSDNode::CXSDNode(NULL, XSD_DOCUMENTATION)
    {
    }
};

#endif // _SCHEMA_DOCUMENATION_HPP_
