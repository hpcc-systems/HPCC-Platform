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

#ifndef _SCHEMA_SIMPLECONTENT_HPP_
#define _SCHEMA_SIMPLECONTENT_HPP_

#include "SchemaCommon.hpp"

class CRestriction;
class CExtension;
class CAnnotation;

class CSimpleContent : public CXSDNode
{
    GETTERSETTER(ID)

public:

    virtual ~CSimpleContent();
    bool checkConstraint(const char *pValue) const;

    const CRestriction* getRestriction() const
    {
        return m_pRestriction;
    }
    const CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }
    const CExtension* getExtension() const
    {
        return m_pExtension;
    }
    virtual void dump(std::ostream& cout, unsigned int offset = 0) const
    {
        UNIMPLEMENTED;
    }
    virtual void getDocumentation(StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }
    virtual void getQML(StringBuffer &strQML, int idx = -1) const
    {
        UNIMPLEMENTED;
    }
    virtual const char* getXML(const char* /*pComponent*/)
    {
        UNIMPLEMENTED;
        return NULL;
    }
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1)
    {
        UNIMPLEMENTED;
    }
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
    {
        UNIMPLEMENTED;
    }
    static CSimpleContent* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CRestriction *m_pRestriction;
    CAnnotation *m_pAnnotation;
    CExtension *m_pExtension;

private:

    CSimpleContent(CXSDNodeBase* pParentNode = NULL, const char* pID = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_SIMPLE_CONTENT), m_strID(pID), m_pRestriction(NULL),
        m_pAnnotation(NULL), m_pExtension(NULL)
    {
    }
};



#endif // _SCHEMA_SIMPLECONTENT_HPP_
