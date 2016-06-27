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

#ifndef _SCHEMA_SIMPLE_TYPE_HPP_
#define _SCHEMA_SIMPLE_TYPE_HPP_

#include "jstring.hpp"
#include "jarray.hpp"
#include "SchemaCommon.hpp"
#include "SchemaRestriction.hpp"

namespace CONFIGURATOR
{

class CRestriction;

class CSimpleType : public CXSDNode
{
public:

    virtual ~CSimpleType()
    {
    }

    virtual CXSDNodeBase* getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName);
    virtual CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName);
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);
    virtual const char* getXML(const char* /*pComponent*/);

    static CSimpleType* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(Name)
    GETTERSETTER(ID)

    const CRestriction* getRestriction() const
    {
        return m_pRestriction;
    }

    void setRestriciton(CRestriction *pRestriction)
    {
        if (m_pRestriction != NULL)
        {
            m_pRestriction->Release();
            m_pRestriction = NULL;
        }
        m_pRestriction = pRestriction;
    }

    bool checkConstraint(const char *pValue) const;

protected:

    CSimpleType(CXSDNodeBase* pRootNode, const char* pName = NULL, const char* pID = NULL, CRestriction* pRestriction = NULL) : CXSDNode::CXSDNode(pRootNode, XSD_SIMPLE_TYPE),m_strName(pName), m_strID(pID), m_pRestriction(pRestriction)
    {
    }

    CRestriction* m_pRestriction;

private:

    CSimpleType() : CXSDNode::CXSDNode(NULL)
    {
    }
};

class CSimpleTypeArray : public CIArrayOf<CSimpleType>, public InterfaceImpl, public CXSDNodeBase
{
public:

    CSimpleTypeArray(CXSDNodeBase* pParentNode) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_SIMPLE_TYPE_ARRAY)
    {
    }

    virtual ~CSimpleTypeArray()
    {
    }

    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);

    static CSimpleTypeArray* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

protected:

private:

    CSimpleTypeArray() : CXSDNodeBase::CXSDNodeBase(NULL)
    {
    }
};

}
#endif // _SCHEMA_SIMPLE_TYPE_HPP_
