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

#ifndef _SCHEMA_DOCUMENATION_HPP_
#define _SCHEMA_DOCUMENATION_HPP_

#include "jiface.hpp"
#include "jstring.hpp"
#include "SchemaCommon.hpp"

class CDocumentation : public CXSDNode
{
public:

    virtual ~CDocumentation()
    {
    }

    GETTERSETTER(DocString)

    const char* getDocumentation() const
    {
        return m_strDocString.str();
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;

    static CDocumentation* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CDocumentation(CXSDNodeBase* pParentNode, const char *pDocs = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_DOCUMENTATION), m_strDocString(pDocs)
    {
    }

private:

    CDocumentation() : CXSDNode::CXSDNode(NULL, XSD_DOCUMENTATION)
    {
    }
};

#endif // _SCHEMA_DOCUMENATION_HPP_
