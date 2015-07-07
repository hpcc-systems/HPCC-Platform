#ifndef _SCHEMA_ATTRIBUTE_GROUP_HPP_
#define _SCHEMA_ATTRIBUTE_GROUP_HPP_

#include "SchemaCommon.hpp"
#include "SchemaAttributes.hpp"

class CAttributeGroup : public CXSDNode
{
public:

    virtual ~CAttributeGroup();

    GETTERSETTER(Name)
    GETTERSETTER(Ref)
    GETTERSETTER(ID)

    virtual const CXSDNodeBase* getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const;
    virtual const CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const;

    const CAttributeArray* getConstAttributeArray() const
    {
        return m_pAttributeArray;
    }

    void setAttributeArray(CAttributeArray *pAttribArray)
    {
        if (m_pAttributeArray != NULL)
        {
            m_pAttributeArray->Release();
        }

        m_pAttributeArray = pAttribArray;
    }

    void setRefNode(CAttributeGroup* pAttributeGroup)
    {
        assert(pAttributeGroup != NULL);

        if (pAttributeGroup != NULL)
        {
            //m_pXSDNode->Release();
        }

        m_pRefAttributeGroup = pAttributeGroup;
    }

    virtual const char* getXML(const char* /*pComponent*/);

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML3(StringBuffer &strQML, int idx = -1) const;

    virtual CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }

    //virtual void loadXML();
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    //virtual void traverseAndProcessNodes() const;

    static CAttributeGroup* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

   CAttributeGroup(CXSDNodeBase* pParentNode = NULL, CAttributeArray *pAttribArray = NULL, CAnnotation *pAnnotation = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_ATTRIBUTE_GROUP), m_pAttributeArray(pAttribArray), m_pRefAttributeGroup(NULL), m_pAnnotation(pAnnotation)
    {
    }

    virtual void setAnnotation(CAnnotation *pAnnotation)
    {
        m_pAnnotation = pAnnotation;
    }


    CAttributeArray* getAttributeArray() const
    {
        return m_pAttributeArray;
    }

    CAttributeGroup *m_pRefAttributeGroup;
    CAttributeArray *m_pAttributeArray;
    CAnnotation     *m_pAnnotation;

private:

};

class CAttributeGroupArray : public CIArrayOf<CAttributeGroup>, public InterfaceImpl, public CXSDNodeBase
{
public:

    CAttributeGroupArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_ATTRIBUTE_GROUP_ARRAY)
    {
    }

    virtual ~CAttributeGroupArray();


    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML2(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML3(StringBuffer &strQML, int idx = -1) const;

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    //virtual void traverseAndProcessNodes() const;

    static CAttributeGroupArray* load(const char* pSchemaFile);
    static CAttributeGroupArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

private:

};

#endif // _SCHEMA_ATTRIBUTE_GROUP_HPP_
