#ifndef _SCHEMA_ANNOTATION_HPP_
#define _SCHEMA_ANNOTATION_HPP_

#include "SchemaCommon.hpp"

class CDocumentation;
class CAppInfo;
class IPropertyTree;

class CAnnotation : public CXSDNode
{
public:

    virtual ~CAnnotation()
    {
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

    //virtual void traverseAndProcessNodes() const;

    const CDocumentation* getDocumentation() const
    {
        return m_pDocumentation;
    }

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    virtual void getQML(StringBuffer &strQML, int idx = -1) const;

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    const CAppInfo* getAppInfo() const
    {
        return m_pAppInfo;
    }

    static CAnnotation* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CAnnotation(CXSDNodeBase* pParentNode, CDocumentation *pDocumenation = NULL, CAppInfo *pAppInfp = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_ANNOTATION), m_pDocumentation(pDocumenation), m_pAppInfo(pAppInfp)
    {
    }

    CDocumentation* m_pDocumentation;
    CAppInfo* m_pAppInfo;

private:

    CAnnotation() : CXSDNode::CXSDNode(NULL, XSD_ANNOTATION)
    {
    }
};

#endif // _SCHEMA_ANNOTATION_HPP_
