#ifndef _SCHEMA_KEY_HPP_
#define _SCHEMA_KEY_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CSelector;
class CFieldArray;
class CAnnotation;

class CKey : public CXSDNode
{
    friend class CKeyArray;
    friend class CKeyRef;

public:

    virtual ~CKey()
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

    const CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }

    static CKey* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(Name)
    GETTERSETTER(ID)

protected:

    CKey(CXSDNodeBase* pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_KEY), m_pFieldArray(NULL), m_pSelector(NULL), m_pAnnotation(NULL)
    {
    }

    virtual bool checkConstraint(const char *pValue) const;

    CFieldArray *m_pFieldArray;
    CSelector *m_pSelector;
    CAnnotation *m_pAnnotation;
};


class CKeyArray : public CIArrayOf<CKey>, public InterfaceImpl, public CXSDNodeBase
{
public:

    virtual ~CKeyArray()
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

    virtual bool checkConstraint(const char *pValue) const;

    static CKeyArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CKeyArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_KEY_ARRAY)
    {
    }

};

#endif // _SCHEMA_KEY_HPP_
