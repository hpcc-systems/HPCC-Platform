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

#ifndef _SCHEMA_LENGTH_HPP_
#define _SCHEMA_LENGTH_HPP_

#include "SchemaCommon.hpp"

namespace CONFIGURATOR
{

class CLength : public CXSDNode
{
public:

    virtual ~CLength()
    {
    }
    static CLength* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }
    virtual const char* getXML(const char* /*pComponent*/)
    {
        UNIMPLEMENTED;
        return NULL;
    }
    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1)
    {
        UNIMPLEMENTED;
    }
    GETTERSETTER(Value)
    GETTERSETTERINT(Length)

private:

    CLength(CXSDNodeBase* pParentNode = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_LENGTH), m_nLength(-1)
    {
    }
};

}
#endif // _SCHEMA_LENGTH_HPP_
