#ifndef _SCHEMA_FIELD_HPP_
#define _SCHEMA_FIELD_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CSchemaField;
class CKey;

class CField : public CXSDNode
{
public:

    virtual ~CField()
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

    static CField* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(ID)
    SETTER(XPath)

    const char* getXPath(bool bRemoveAmpersand = true) const
    {
        if (bRemoveAmpersand == true)
        {
            static StringBuffer strRetString(m_strXPath);

            static bool bOnce = true;

            if (bOnce == true)
            {
                strRetString.remove(0,1);
            }

            return strRetString;
        }
        else
        {
            return m_strXPath.str();
        }
    }


protected:

    CField(CXSDNodeBase *pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_FIELD)
    {
    }

    StringBuffer m_strXPath;
};


class CFieldArray : public CIArrayOf<CField>, public InterfaceImpl, public CXSDNodeBase
{
public:

    virtual ~CFieldArray()
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

    static CFieldArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CFieldArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_FIELD_ARRAY)
    {
    }
};


#endif // _SCHEMA_FIELD_HPP_
