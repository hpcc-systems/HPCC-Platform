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

#ifndef _SCHEMA_SCHEMA_HPP_
#define _SCHEMA_SCHEMA_HPP_

#include "jarray.hpp"
#include "jstring.hpp"
#include "SchemaAll.hpp"
#include "SchemaCommon.hpp"

class CSchema : public InterfaceImpl, public CXSDNodeBase
{
public:

    virtual ~CSchema();

    GETTERSETTER(XMLNS_XS)
    GETTERSETTER(ElementFormDefault)
    GETTERSETTER(AttributeFormDefault)

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML2(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML3(StringBuffer &strQML, int idx = -1) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual const char* getXML(const char* /*pComponent*/);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);

    CXSDNode* getExtensionType(const char* pExtensionTypeName) const;

    const char* getSchemaLocation() const
    {
        return m_strSchemaLocation.str();
    }

    const char* getSchemaFileName() const;

    CComplexTypeArray* getComplexTypeArray() const
    {
        return m_pComplexTypeArray;
    }

    virtual CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }

    static CSchema* load(const char* pSchemaLocation, const IPropertyTree *pSchemaRoot, const char* xpath);
    static CSchema* load(const char* pSchemaLocation, CXSDNodeBase* pParentNode);

protected:

    CSchema(const char * pSchemaLocation, const char* pXMLNS_XS = NULL, const char* pElementFormDefault = NULL, const char* pAttributeFormDefault = NULL,
            CElementArray* pElementArray = NULL, CComplexTypeArray* pComplexTypeArray = NULL, CAttributeGroupArray* pAttributeGroupArray = NULL,
            CSimpleTypeArray* pSimpleTypeArray = NULL, CIncludeArray* pIncludeArray = NULL, CAnnotation *pAnnotation = NULL) : CXSDNodeBase::CXSDNodeBase(NULL, XSD_SCHEMA), m_strSchemaLocation(pSchemaLocation),
                m_strXMLNS_XS(pXMLNS_XS), m_strElementFormDefault(pElementFormDefault), m_strAttributeFormDefault(pAttributeFormDefault),
                m_pElementArray(pElementArray), m_pComplexTypeArray(pComplexTypeArray), m_pAttributeGroupArray(pAttributeGroupArray),
                m_pSimpleTypeArray(pSimpleTypeArray), m_pIncludeArray(pIncludeArray), m_pAnnotation(pAnnotation)
    {
    }

    StringBuffer            m_strSchemaLocation;
    CElementArray*          m_pElementArray;
    CComplexTypeArray*      m_pComplexTypeArray;
    CAttributeGroupArray*   m_pAttributeGroupArray;
    CSimpleTypeArray*       m_pSimpleTypeArray;
    CIncludeArray*          m_pIncludeArray;
    CAnnotation*            m_pAnnotation;

private:
};



#endif // _SCHEMA_SCHEMA_HPP_
