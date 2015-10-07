/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef _SCHEMA_ENUMERATION_HPP_
#define _SCHEMA_ENUMERATION_HPP_

#include "SchemaCommon.hpp"

class CEnumeration : public CXSDNode
{
public:
    friend class CEnumerationArray;

    virtual ~CEnumeration()
    {
    }

    GETTERSETTER(Value)

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual const char* getXML(const char* /*pComponent*/);
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);
    bool isInstanceValueValid() const
    {
        return m_bInstanceValueValid;
    }
    static CEnumeration* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

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
