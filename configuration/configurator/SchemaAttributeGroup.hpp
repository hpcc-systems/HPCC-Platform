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

#ifndef _SCHEMA_ATTRIBUTE_GROUP_HPP_
#define _SCHEMA_ATTRIBUTE_GROUP_HPP_

#include "SchemaCommon.hpp"
#include "SchemaAttributes.hpp"

namespace CONFIGURATOR
{

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
            m_pAttributeArray->Release();

        m_pAttributeArray = pAttribArray;
    }

    void setRefNode(CAttributeGroup* pAttributeGroup)
    {
        assert(pAttributeGroup != NULL);
        m_pRefAttributeGroup = pAttributeGroup;
    }

    virtual const char* getXML(const char* /*pComponent*/);
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const;
    virtual void getJSON(::StringBuffer &strJSON, unsigned int offset = 0, int idx = -1) const;

    virtual CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }

    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);

    static CAttributeGroup* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

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

class CAttributeGroupArray : public ::CIArrayOf<CAttributeGroup>, public InterfaceImpl, public CXSDNodeBase
{
public:

    CAttributeGroupArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_ATTRIBUTE_GROUP_ARRAY)
    {
    }

    virtual ~CAttributeGroupArray();
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const;
    virtual void getJSON(::StringBuffer &strJSON, unsigned int offset = 0, int idx = -1) const;
    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);
    static CAttributeGroupArray* load(const char* pSchemaFile);
    static CAttributeGroupArray* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

protected:
private:
};

}
#endif // _SCHEMA_ATTRIBUTE_GROUP_HPP_
