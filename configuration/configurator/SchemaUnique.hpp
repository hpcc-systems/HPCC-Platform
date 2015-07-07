#ifndef _SCHEMA_UNIQUE_HPP_
#define _SCHEMA_UNIQUE_HPP_

#include "SchemaCommon.hpp"
#include "SchemaKey.hpp"
#include "jstring.hpp"

class CSchemaUnique;

class CUnique : public CXSDNode
{
public:

    virtual ~CUnique()
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

    //virtual const char* getXML(const char* /*pComponent*/);

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1)
    {
        assert(!"Not Implemented");
    }

    static CUnique* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(ID)
    GETTERSETTER(Name)

protected:

    CUnique(CXSDNodeBase *pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_UNIQUE)
    {
    }

};


class CUniqueArray : public CIArrayOf<CUnique>, public InterfaceImpl, public CXSDNodeBase
{
public:

    virtual ~CUniqueArray()
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

    static CUniqueArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CUniqueArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_UNIQUE_ARRAY)
    {
    }
};



#endif // _SCHEMA_UNIQUE_HPP_
