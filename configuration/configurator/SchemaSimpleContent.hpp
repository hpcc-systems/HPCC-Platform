#ifndef _SCHEMA_SIMPLECONTENT_HPP_
#define _SCHEMA_SIMPLECONTENT_HPP_

#include "SchemaCommon.hpp"

class CRestriction;
class CExtension;
class CAnnotation;

class CSimpleContent : public CXSDNode
{
    GETTERSETTER(ID)

public:

    virtual ~CSimpleContent();

    bool checkConstraint(const char *pValue) const;

    const CRestriction* getRestriction() const
    {
        return m_pRestriction;
    }

    const CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }

    const CExtension* getExtension() const
    {
        return m_pExtension;
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const
    {
        assert(!"Not Implemented");
    }

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

    virtual const char* getXML(const char* /*pComponent*/)
    {
        assert(!"Not Implemented");
        return NULL;
    }

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1)
    {
        assert(!"Not Implemented");
    }

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
    {
        assert(!"Not Implemented");
    }

    static CSimpleContent* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CRestriction *m_pRestriction;
    CAnnotation *m_pAnnotation;
    CExtension *m_pExtension;

private:

    CSimpleContent(CXSDNodeBase* pParentNode = NULL, const char* pID = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_SIMPLE_CONTENT), m_strID(pID), m_pRestriction(NULL),
        m_pAnnotation(NULL), m_pExtension(NULL)
    {
    }
};



#endif // _SCHEMA_SIMPLECONTENT_HPP_
