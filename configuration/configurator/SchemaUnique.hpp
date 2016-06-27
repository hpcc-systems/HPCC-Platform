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

#ifndef _SCHEMA_UNIQUE_HPP_
#define _SCHEMA_UNIQUE_HPP_

#include "SchemaCommon.hpp"
#include "SchemaKey.hpp"
#include "jstring.hpp"

namespace CONFIGURATOR
{

class CSchemaUnique;

class CUnique : public CXSDNode
{
public:

    virtual ~CUnique()
    {
    }

    virtual void dump(::std::ostream &cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }

    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1)
    {
        UNIMPLEMENTED;
    }
    static CUnique* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(ID)
    GETTERSETTER(Name)

protected:

    CUnique(CXSDNodeBase *pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_UNIQUE)
    {
    }

};

class CUniqueArray : public CIArrayOf<CUnique>, public InterfaceImpl, public CXSDNodeBase
{
public:

    virtual ~CUniqueArray()
    {
    }

    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }

    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1)
    {
        UNIMPLEMENTED;
    }
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
    {
        UNIMPLEMENTED;
    }
    static CUniqueArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CUniqueArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_UNIQUE_ARRAY)
    {
    }
};

}
#endif // _SCHEMA_UNIQUE_HPP_
