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

#ifndef _SCHEMA_FIELD_HPP_
#define _SCHEMA_FIELD_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

namespace CONFIGURATOR
{

class CSchemaField;
class CKey;

class CField : public CXSDNode
{
public:

    virtual ~CField()
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

    static CField* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(ID)
    SETTER(XPath)

    const char* getXPath(bool bRemoveAmpersand = true) const
    {
        if (bRemoveAmpersand == true)
        {
            static ::StringBuffer strRetString(m_strXPath);
            static bool bOnce = true;

            if (bOnce == true)
                strRetString.remove(0,1);

            return strRetString;
        }
        else
            return m_strXPath.str();
    }


protected:

    CField(CXSDNodeBase *pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_FIELD)
    {
    }

    ::StringBuffer m_strXPath;
};

class CFieldArray : public ::CIArrayOf<CField>, public InterfaceImpl, public CXSDNodeBase
{
public:
    virtual ~CFieldArray()
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
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
    {
        UNIMPLEMENTED;
    }

    static CFieldArray* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CFieldArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_FIELD_ARRAY)
    {
    }
};

}
#endif // _SCHEMA_FIELD_HPP_
