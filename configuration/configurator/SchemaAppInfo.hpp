﻿#ifndef _SCHEMA_APP_INFO_HPP_
#define _SCHEMA_APP_INFO_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class IPropertyTree;

class CAppInfo : public CXSDNodeBase
{
public:

    virtual ~CAppInfo()
    {
    }

    GETTERSETTER(ViewType)
    GETTERSETTER(ColIndex)
    GETTERSETTER(ToolTip)
    GETTERSETTER(Title)
    GETTERSETTER(AutoGenForWizard)
    GETTERSETTER(AutoGenDefaultValue)
    GETTERSETTER(Width)
    GETTERSETTER(AutoGenDefaultValueForMultiNode)
    GETTERSETTER(ViewChildNodes)
    GETTERSETTER(XPath)
    GETTERSETTER(DocTableID)


    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const;

    virtual void getDojoJS(StringBuffer &strJS) const;

    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    bool getDocLineBreak() const
    {
        return m_bDocLineBreak;
    }

    //virtual void traverseAndProcessNodes() const;

    static CAppInfo* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CAppInfo(CXSDNodeBase* pParentNode, const char *pViewType = NULL, const char *pColIndex = NULL, const char* pToolTip = NULL, const char* pTitle = NULL, const char* pWidth = NULL, const char* pAutoGenForWizard = NULL,\
             const char* pAutoGenDefaultValue = NULL, const char* pAutoGenDefaultForMultiNode = NULL, const char* pViewChildNodes = NULL, const char* pXPath = NULL, const char* pDocTableID = NULL, bool bDocLineBreak = false)\
        : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_APP_INFO), m_strViewType(pViewType), m_strColIndex(pColIndex), m_strToolTip(pToolTip), m_strTitle(pTitle), m_strWidth(pWidth), m_strAutoGenForWizard(pAutoGenForWizard),\
          m_strAutoGenDefaultValue(pAutoGenDefaultValue), m_strAutoGenDefaultValueForMultiNode(pAutoGenDefaultForMultiNode), m_strViewChildNodes(pViewChildNodes), m_strXPath(pXPath), m_strDocTableID(pDocTableID),\
          m_bDocLineBreak(bDocLineBreak)
    {
    }

    bool m_bDocLineBreak;

private:

    CAppInfo(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_APP_INFO)
    {
    }
};

#endif // _SCHEMA_APP_INFO_HPP_
