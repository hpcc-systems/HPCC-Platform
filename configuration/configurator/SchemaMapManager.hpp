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

#include "jhash.hpp"
#include "jhash.ipp"
#include "jlog.hpp"
#include "SchemaCommon.hpp"

#ifndef _SCHEMA_MAP_MANAGER_HPP_
#define _SCHEMA_MAP_MANAGER_HPP_

class CSchema;
class CAttribute;
class CAttributeGroup;
class CSimpleType;
class CComplexType;
class CAttributeGroup;
class CAttribute;
class CRestriction;
class CElementArray;
class CElement;
class CKey;

class CSchemaMapManager
{
public:

    CSchemaMapManager();
    virtual ~CSchemaMapManager();

    CSchema* getSchemaForXSD(const char* pComponent);
    void setSchemaForXSD(const char* pComponent, CSchema *pSchema);

    CSimpleType* getSimpleTypeWithName(const char* pName);
    void setSimpleTypeWithName(const char* pName, CSimpleType *pSimpleType);

    CComplexType* getComplexTypeWithName(const char* pName);
    void setComplexTypeWithName(const char* pName, CComplexType *pComplexType);
    CComplexType* getComplexTypeFromXPath(const char *pXPath);

    CAttributeGroup *getAttributeGroup(const char* pName);
    void setAttributeGroupTypeWithName(const char* pName, CAttributeGroup *pAttributeGroup);
    CAttributeGroup* getAttributeGroupFromXPath(const char *pXPath);

    CElement* getElementWithName(const char* pName);
    void setElementWithName(const char* pName, CElement *pElement);

    void addMapOfXPathToAttribute(const char* pXPath, CAttribute *pAttribute);
    void removeMapOfXPathToAttribute(const char* pXPath);
    CAttribute* getAttributeFromXPath(const char* pXPath);

    void addMapOfXPathToRestriction(const char*pXPath, CRestriction *pRestriction);
    void removeMapOfXPathToRestriction(const char*pXPath);
    CRestriction* getRestrictionFromXPath(const char* pXPath);

    void addMapOfXSDXPathToElementArray(const char* pXPath, CElementArray *pElementArray);
    void removeMapOfXSDXPathToElementArray(const char* pXPath);
    CElementArray* getElementArrayFromXSDXPath(const char* pXPath);

    void addMapOfXPathToElement(const char* pXPath, CElement *pElement, bool bIsTopLevelElement = false);
    void removeMapOfXPathToElement(const char* pXPath);
    CElement* getElementFromXPath(const char *pXPath);

    void addMapOfXSDXPathToElement(const char* pXPath, CElement *pElement);
    CElement* getElementFromXSDXPath(const char *pXPath) const;

    void addMapOfXSDXPathToKey(const char* pXPath, CKey *pKey);
    CKey* getKeyFromXSDXPath(const char *pXPath) const;

    int getNumberOfComponents() const;
    CElement* getComponent(int index);
    int getIndexOfElement(const CElement *pElem);

    enum NODE_TYPES getEnumFromTypeName(const char *pTypeName) const;
    const char* getTypeNameFromEnum(enum NODE_TYPES, bool bForDump = false) const;

protected:

    typedef MapStringTo<CSchema*> MapStringToCSchema;
    Owned<MapStringToCSchema> m_pSchemaPtrMap;

    typedef MapStringTo<CSimpleType*> MapStringToCSimpleType;
    Owned<MapStringToCSimpleType> m_pSimpleTypePtrMap;

    typedef MapStringTo<CComplexType*> MapStringToCComplexType;
    Owned<MapStringToCComplexType> m_pComplexTypePtrsMap;

    typedef MapStringTo<CAttributeGroup*> MapStringToCAttributeGroup;
    Owned<MapStringToCAttributeGroup> m_pAttributeGroupTypePtrsMap;

    typedef MapStringTo<CAttribute*> MapStringToCAttribute;
    Owned<MapStringToCAttribute> m_pAttributePtrsMap;

    typedef MapStringTo<CRestriction*> MapStringToCRestriction;
    Owned<MapStringToCRestriction> m_pRestrictionPtrsMap;

    typedef MapStringTo<CElement*> MapStringToCElement;
    Owned<MapStringToCElement> m_pElementPtrsMap;
    Owned<MapStringToCElement> m_pElementNamePtrsMap;
    Owned<MapStringToCElement> m_pXSDToElementPtrsMap;

    typedef MapStringTo<CElementArray*> MapStringToCElementArray;
    Owned<MapStringToCElementArray> m_pElementArrayPtrsMap;

    const char* m_enumArray[XSD_ERROR][2];

    typedef MapStringTo<enum NODE_TYPES> MapStringToNodeTypeEnum;
    Owned<MapStringToNodeTypeEnum> m_pStringToEnumMap;

    typedef MapStringTo<CKey*> MapStringToCKey;
    Owned<MapStringToCKey> m_pStringToKeyPtrsMap;

    typedef MapStringTo<CXSDNodeBase*> MapStringToCNodeBase;
    Owned<MapStringToCNodeBase> m_pStringToNodeBaseMap;

    struct STypeStrings
    {
        const char *pXSDTypeString;
        const char *pDumpTypeString;
    };

private:
};

#endif // _SCHEMA_MAP_MANAGER_HPP_
