#ifndef _SCHEMA_KEYREF_HPP_
#define _SCHEMA_KEYREF_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CSelector;
class CFieldArray;

class CKeyRef : public CXSDNode
{
public:

    virtual ~CKeyRef()
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

    //virtual void traverseAndProcessNodes() const;

    //virtual const char* getXML(const char* /*pComponent*/);

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1)
    {
        assert(!"Not Implemented");
    }

    static CKeyRef* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    bool checkConstraint(const char *pValue) const;

    GETTERSETTER(Name)
    GETTERSETTER(ID)
    GETTERSETTER(Refer)

protected:

    CKeyRef(CXSDNodeBase* pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_KEYREF), m_pFieldArray(NULL), m_pSelector(NULL)
    {
    }

    CFieldArray *m_pFieldArray;
    CSelector *m_pSelector;
};


class CKeyRefArray : public CIArrayOf<CKeyRef>, public InterfaceImpl, public CXSDNodeBase
{
public:

    virtual ~CKeyRefArray()
    {
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

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

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
    {
        assert(!"Not Implemented");
    }

    static CKeyRefArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CKeyRefArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_KEYREF_ARRAY)
    {
    }

};

#endif // _SCHEMA_KeyRef_HPP_
