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

#ifndef _SCHEMA_EXTENSION_HPP_
#define _SCHEMA_EXTENSION_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

namespace CONFIGURATOR
{

class IPropertyTree;

class CExtension : public CXSDNodeWithBase
{
public:

    virtual ~CExtension()
    {
    }

    virtual const char* getXML(const char* /*pComponent*/);
    virtual void initExtension();
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const
    {
       throwUnexpected(); // Should not be called directly
    }

    static CExtension* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CExtension(CXSDNodeBase* pParentNode, const char* pBase = NULL, CXSDNode *pXSDNode = NULL) : CXSDNodeWithBase::CXSDNodeWithBase(pParentNode, XSD_EXTENSION)
    {
    }

private:

    CExtension() : CXSDNodeWithBase::CXSDNodeWithBase(NULL, XSD_EXTENSION)
    {
    }
};

}
#endif // _SCHEMA_EXTENSION_HPP_
