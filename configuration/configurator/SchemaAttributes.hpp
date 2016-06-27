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

#ifndef _SCHEMA_ATTRIBUTES_HPP_
#define _SCHEMA_ATTRIBUTES_HPP_

#include "jarray.hpp"
#include "jatomic.hpp"
#include "XMLTags.h"
#include "SchemaCommon.hpp"
#include "SchemaAnnotation.hpp"

namespace CONFIGURATOR
{


class CSimpleTypeArray;
class CKeyRefArray;
class CKeyArray;
class CKey;
class CKeyRef;

class CAttribute : public CXSDNodeWithType
{
public:

    CAttribute(CXSDNodeBase* pParentNode, const char* pName = NULL) : CXSDNodeWithType::CXSDNodeWithType(pParentNode, XSD_ATTRIBUTE), m_strName(pName),
            m_strDefault(""), m_strUse(""), m_pAnnotation(NULL), m_pSimpleTypeArray(NULL), m_bInstanceValueValid(false)
    {
    }

    CAttribute(CXSDNodeBase* pParentNode, const char* pName, const char* pType, const char* pDefault, const char* pUse) : CXSDNodeWithType::CXSDNodeWithType(pParentNode, XSD_ATTRIBUTE), m_strName(pName), m_pAnnotation(NULL),
            m_strDefault(pDefault), m_strUse(pUse), m_pSimpleTypeArray(NULL), m_bInstanceValueValid(false)
    {
    }

    virtual ~CAttribute();

    GETTERSETTER(Name)
    GETTERSETTER(Default)
    GETTERSETTER(Use)
    GETTERSETTER(InstanceValue)

    const CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }

    const char* getTitle() const;
    virtual const char* getXML(const char* pComponent);
    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const;
    virtual void getJSON(::StringBuffer &strJSON, unsigned int offset = 0, int idx = -1) const;
    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);

    const CSimpleTypeArray* getSimpleTypeArray() const
    {
        return m_pSimpleTypeArray;
    }

    static CAttribute* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath = NULL);

    bool isInstanceValueValid()
    {
        return m_bInstanceValueValid;
    }

    bool isHidden();
    virtual bool setEnvValueFromXML(const char *p);
    void appendReverseKey(const CKey *pKey);
    void appendReverseKeyRef(const CKeyRef *pKeyRef);

protected:

    void setInstanceAsValid(bool bValid = true)
    {
        m_bInstanceValueValid = bValid;
    }

    void setAnnotation(CAnnotation *pAnnotation)
    {
        assert(pAnnotation != NULL);  // why would this ever be NULL?
        m_pAnnotation = pAnnotation;
    }

    void setSimpleTypeArray(CSimpleTypeArray *pSimpleTypeArray)
    {
        assert(pSimpleTypeArray != NULL);  // why would this ever be NULL?
        m_pSimpleTypeArray = pSimpleTypeArray;
    }

    CAnnotation *m_pAnnotation;
    CSimpleTypeArray *m_pSimpleTypeArray;
    ::PointerArray m_ReverseKeyArray;
    ::PointerArray m_ReverseKeyRefArray;
    bool m_bInstanceValueValid;

private:

};

class CAttributeArray : public ::CIArrayOf<CAttribute>, public InterfaceImpl, public CXSDNodeBase
{
public:

    CAttributeArray(CXSDNodeBase* pParentNode = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_ATTRIBUTE_ARRAY)
    {
    }

    virtual ~CAttributeArray()
    {
    }

    virtual void dump(::std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(::StringBuffer &strDoc) const;
    virtual void getJSON(::StringBuffer &strJSON, unsigned int offset = 0, int idx = -1) const;
    virtual void populateEnvXPath(::StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree);
    virtual const char* getXML(const char* /*pComponent*/);
    const CAttribute* findAttributeWithName(const char *pName, bool bCaseInsensitive = true) const;
    const void getAttributeNames(::StringArray &names, ::StringArray &titles) const;
    static CAttributeArray* load(const char* pSchemaFile);
    static CAttributeArray* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    bool getCountOfValueMatches(const char *pValue) const;

private:
};

}
#endif // _SCHEMA_ATTRIBUTES_HPP_
