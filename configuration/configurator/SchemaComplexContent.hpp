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

#ifndef _SCHEMA_COMPLEX_CONTENT_HPP_
#define _SCHEMA_COMPLEX_CONTENT_HPP_

#include "SchemaCommon.hpp"

class IPropertyTree;
class CExtension;

class CComplexContent : public CXSDNode
{
public:

    virtual ~CComplexContent()
    {
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual const char* getXML(const char* /*pComponent*/);

    static CComplexContent* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CComplexContent(CXSDNodeBase* pParentNode, CExtension *pExtension = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_COMPLEX_CONTENT), m_pExtension(pExtension)
    {
    }

    CExtension *m_pExtension;

private:

    CComplexContent(CXSDNodeBase* pParentNode = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_COMPLEX_CONTENT)
    {
    }
};

#endif // _SCHEMA_COMPLEX_CONTENT_HPP_
