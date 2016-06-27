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

#ifndef _SCHEMA_ANNOTATION_HPP_
#define _SCHEMA_ANNOTATION_HPP_

#include "SchemaCommon.hpp"

namespace CONFIGURATOR
{

class CDocumentation;
class CAppInfo;

class CAnnotation : public CXSDNode
{
public:

    virtual ~CAnnotation()
    {
    }

    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;

    const CDocumentation* getDocumentation() const
    {
        return m_pDocumentation;
    }

    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);

    const CAppInfo* getAppInfo() const
    {
        return m_pAppInfo;
    }
    static CAnnotation* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CAnnotation(CXSDNodeBase* pParentNode, CDocumentation *pDocumenation = NULL, CAppInfo *pAppInfp = NULL) : CXSDNode::CXSDNode(pParentNode, XSD_ANNOTATION), m_pDocumentation(pDocumenation), m_pAppInfo(pAppInfp)
    {
    }

    CDocumentation* m_pDocumentation;
    CAppInfo* m_pAppInfo;

private:

    CAnnotation() : CXSDNode::CXSDNode(NULL, XSD_ANNOTATION)
    {
    }
};
}
#endif // _SCHEMA_ANNOTATION_HPP_
