#ifndef _SCHEMA_CHOICE_HPP_
#define _SCHEMA_CHOICE_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CElementArray;

class CChoice : public CXSDNode
{
public:

    virtual ~CChoice()
    {
    }

    GETTERSETTER(MaxOccurs)
    GETTERSETTER(MinOccurs)
    GETTERSETTER(ID)

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    virtual void getQML(StringBuffer &strQML, int idx = -1) const;

    virtual const char* getXML(const char* /*pComponent*/);

    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    static CChoice* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CChoice(CXSDNodeBase* pParentNode, CElementArray *pElemArray = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_CHOICE), m_pElementArray(pElemArray)
    {
    }

    CElementArray *m_pElementArray;

private:

    CChoice() : CXSDNode::CXSDNode(NULL, XSD_CHOICE)
    {
    }
};

#endif // _SCHEMA_CHOICE_HPP_
