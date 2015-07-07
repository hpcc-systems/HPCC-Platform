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

#ifndef _SCHEMA_KEYREF_HPP_
#define _SCHEMA_KEYREF_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CSelector;
class CFieldArray;

class CKeyRef : public CXSDNode
{
public:

    virtual ~CKeyRef()
    {
    }

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    bool checkConstraint(const char *pValue) const;

    virtual void getDocumentation(StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }
    virtual void getQML(StringBuffer &strQML, int idx = -1) const
    {
        UNIMPLEMENTED;
    }

    static CKeyRef* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(Name)
    GETTERSETTER(ID)
    GETTERSETTER(Refer)

protected:

    CKeyRef(CXSDNodeBase* pParentNode) : CXSDNode::CXSDNode(pParentNode, XSD_KEYREF), m_pFieldArray(NULL), m_pSelector(NULL)
    {
    }

    CFieldArray *m_pFieldArray;
    CSelector *m_pSelector;
};

class CKeyRefArray : public CIArrayOf<CKeyRef>, public InterfaceImpl, public CXSDNodeBase
{
public:
    virtual ~CKeyRefArray()
    {
    }

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);

    virtual void getDocumentation(StringBuffer &strDoc) const
    {
        UNIMPLEMENTED;
    }
    virtual void getQML(StringBuffer &strQML, int idx = -1) const
    {
        UNIMPLEMENTED;
    }
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
    {
        UNIMPLEMENTED;
    }
    static CKeyRefArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CKeyRefArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_KEYREF_ARRAY)
    {
    }
};

#endif // _SCHEMA_KeyRef_HPP_
