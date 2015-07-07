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

#ifndef _SCHEMA_CHOICE_HPP_
#define _SCHEMA_CHOICE_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class CElementArray;

class CChoice : public CXSDNode
{
public:

    virtual ~CChoice()
    {
    }

    GETTERSETTER(MaxOccurs)
    GETTERSETTER(MinOccurs)
    GETTERSETTER(ID)

    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual const char* getXML(const char* /*pComponent*/);
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    static CChoice* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CChoice(CXSDNodeBase* pParentNode, CElementArray *pElemArray = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_CHOICE), m_pElementArray(pElemArray)
    {
    }

    CElementArray *m_pElementArray;

private:

    CChoice() : CXSDNode::CXSDNode(NULL, XSD_CHOICE)
    {
    }
};

#endif // _SCHEMA_CHOICE_HPP_
