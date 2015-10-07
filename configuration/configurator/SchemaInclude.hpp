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

#ifndef _SCHEMA_INCLUDE_HPP_
#define _SCHEMA_INCLUDE_HPP_

#include "jstring.hpp"
#include "jarray.hpp"
#include "SchemaCommon.hpp"

class CSchema;

class CInclude : public CXSDNode
{
public:

    virtual ~CInclude()
    {
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual const char* getXML(const char* /*pComponent*/);
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    GETTERSETTER(SchemaLocation)

    const CSchema* getIncludeSchema() const
    {
        return m_pIncludedSchema;
    }

    static CInclude* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CInclude(CXSDNodeBase* pParentNode, const char* pSchemaLocation = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_INCLUDE), m_strSchemaLocation(pSchemaLocation)
    {
    }

    CSchema *m_pIncludedSchema;

    void setIncludedSchema(CSchema *pSchema)
    {
        m_pIncludedSchema = pSchema;
    }

private:

    CInclude(CXSDNodeBase* pParentNode = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_INCLUDE), m_pIncludedSchema(NULL)
    {
    }

};

class CIncludeArray : public CIArrayOf<CInclude>, public InterfaceImpl, public CXSDNodeBase
{
public:

    CIncludeArray(CXSDNodeBase* pParentNode, IPropertyTree *pSchemaRoot) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_INCLUDE_ARRAY), m_pSchemaRoot(pSchemaRoot)
    {
    }

    virtual ~CIncludeArray()
    {
    }

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual const char* getXML(const char* /*pComponent*/);

    static CIncludeArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    IPropertyTree *m_pSchemaRoot;

private:

    CIncludeArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_INCLUDE_ARRAY)
    {
    }
};

#endif // _SCHEMA_INCLUDE_HPP_
