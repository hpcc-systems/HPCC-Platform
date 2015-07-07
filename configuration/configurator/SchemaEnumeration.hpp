#ifndef _SCHEMA_ENUMERATION_HPP_
#define _SCHEMA_ENUMERATION_HPP_

#include "SchemaCommon.hpp"

class CEnumeration : public CXSDNode
{
public:

    virtual ~CEnumeration()
    {
    }

    GETTERSETTER(Value)

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    void getQML(StringBuffer &strQML, int idx = -1) const;


    virtual const char* getXML(const char* /*pComponent*/);

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    bool isInstanceValueValid() const
    {
        return m_bInstanceValueValid;
    }

    static CEnumeration* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    friend class CEnumerationArray;

protected:

    CEnumeration(CXSDNodeBase* pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_ENUMERATION), m_strValue(""), m_bInstanceValueValid(false)
    {
    }

    void setInstanceValueValid(bool b)
    {
        m_bInstanceValueValid = b;
    }

    bool m_bInstanceValueValid;

private:

    CEnumeration() : CXSDNode::CXSDNode(NULL, XSD_ENUMERATION)
    {
    }
};

class CEnumerationArray : public CIArrayOf<CEnumeration>, public InterfaceImpl, public CXSDNodeBase
{
public:

    CEnumerationArray(CXSDNodeBase* pParentNode, IPropertyTree *pSchemaRoot = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_ENUMERATION_ARRAY), m_pSchemaRoot(pSchemaRoot)
    {
    }

    virtual ~CEnumerationArray()
    {
    }

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    void getQML(StringBuffer &strQML, int idx = -1) const;

    virtual const char* getXML(const char* /*pComponent*/);

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    int getEnvValueNodeIndex() const;

    void setEnvValueNodeIndex(int index);

    static CEnumerationArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    IPropertyTree *m_pSchemaRoot;

private:

    CEnumerationArray() : CXSDNodeBase::CXSDNodeBase(NULL, XSD_ENUMERATION_ARRAY)
    {
    }
};

#endif // _SCHEMA_ENUMERATION_HPP_
