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

#ifndef _SCHEMA_SEQUENCE_HPP_
#define _SCHEMA_SEQUENCE_HPP_

#include "SchemaCommon.hpp"

class CElementArray;
class IPropertyTree;

class CSequence : public CXSDNode
{
public:

    CSequence(CXSDNodeBase* pParentNode = NULL, CElementArray* pElemArray = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_SEQUENCE), m_pElementArray(pElemArray)
    {
    }
    virtual ~CSequence()
    {
    }
    virtual const CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const;
    virtual void dump(std::ostream& cout, unsigned int offset  = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML2(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML3(StringBuffer &strQML, int idx = -1) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);
    bool hasChildElements() const;

    static CSequence* load(CXSDNodeBase* pRootNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CElementArray *m_pElementArray;

private:
};

#endif // _SCHEMA_SEQUENCE_HPP_
