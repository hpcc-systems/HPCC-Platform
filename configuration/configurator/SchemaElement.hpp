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

#ifndef _SCHEMA_ELEMENT_HPP_
#define _SCHEMA_ELEMENT_HPP_

#include "jiface.hpp"
#include "jstring.hpp"
#include "jlib.hpp"
#include "jarray.hpp"
#include "SchemaCommon.hpp"
#include "SchemaComplexType.hpp"
#include <climits>

class CAnnotation;
class CComplexTypeArray;
class IPropertyTree;
class CKeyArray;
class CKeyRefArray;
class CKeyRef;
class CSchema;
class CElementArray;
class CSimpleType;

static const char* DEFAULT_ELEMENT_ARRAY_XPATH(".");

class CElement : public CXSDNodeWithType
{
public:

    virtual ~CElement()
    {
        assert(false);
    }

    virtual const CXSDNodeBase* getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const;
    virtual const CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const;
    virtual const char* getXML(const char* /*pComponent*/);
    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strJS) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML2(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML3(StringBuffer &strQML, int idx = -1) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);
    virtual bool isTopLevelElement() const;
    const CSchema* getConstSchemaNode() const;

    void setTopLevelElement(bool b = true)
    {
        m_bTopLevelElement =  b;
    }
    void setParentIndex(int index)
    {
        m_nParentIndex = index;
    }
    int getParentIndex() const
    {
        return m_nParentIndex;
    }
    const CAnnotation* getAnnotation() const
    {
        return m_pAnnotation;
    }
    const CSimpleType* getSimpleType() const
    {
        return m_pSimpleType;
    }
    const CComplexTypeArray* getComplexTypeArray() const
    {
        return m_pComplexTypeArray;
    }

    static const CXSDNodeBase* getAncestorElement(const CXSDNodeBase *pNode)
    {
         return pNode->getParentNodeByType(XSD_ELEMENT);
    }
    static const CElement* getTopMostElement(const CXSDNodeBase *pNode);
    static bool isAncestorTopElement(const CXSDNodeBase *pNode)
    {
        return (pNode != NULL && pNode->getParentNodeByType(XSD_ELEMENT) == getTopMostElement(pNode));
    }
    void setRefElementNode(CElement *pElement)
    {
        assert (pElement != NULL && this->getRefElementNode() != NULL);
        if (pElement != NULL)
            this->m_pElementRefNode = pElement;
    }
    CElement* getRefElementNode() const
    {
        return this->m_pElementRefNode;
    }

    bool isATab() const;
    bool isLastTab(const int idx) const;
    bool getIsInXSD() const
    {
        return m_bIsInXSD;
    }

    bool hasChildElements() const;

    int getMaxOccursInt() const
    {
        if (strcmp(m_strMaxOccurs.str(), TAG_UNBOUNDED) == 0)
            return __SHRT_MAX__;
        else
            return atoi(m_strMaxOccurs.str());
    }

    int getMinOccursInt() const
    {
        return atoi(m_strMinOccurs.str());
    }

    static CElement* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath, bool bIsInXSD = true);

    const char * getViewType() const;

    GETTERSETTER(Name)
    GETTERSETTER(MaxOccurs)
    GETTERSETTER(MinOccurs)
    GETTERSETTER(Title)
    GETTERSETTER(InstanceName)
    GETTERSETTER(Ref)

protected:

    CElement(CXSDNodeBase* pParentNode, const char* pName = "") : CXSDNodeWithType::CXSDNodeWithType(pParentNode, XSD_ELEMENT), m_strMinOccurs("1"), m_strMaxOccurs("1"), m_strName(pName), m_pAnnotation(NULL),
        m_pComplexTypeArray(NULL), m_pKeyArray(NULL), m_pKeyRefArray(NULL), m_pReverseKeyRefArray(NULL), m_pElementRefNode(NULL), m_pSimpleType(NULL),\
        m_bTopLevelElement(false), m_nParentIndex(-1), m_bIsInXSD(true)
    {
    }

    void setIsInXSD(bool b);

    CAnnotation * m_pAnnotation;
    CComplexTypeArray* m_pComplexTypeArray;
    CKeyArray *m_pKeyArray;
    CKeyRefArray *m_pKeyRefArray;
    CKeyRefArray *m_pReverseKeyRefArray;
    CElement *m_pElementRefNode;
    CSimpleType *m_pSimpleType;

    bool m_bTopLevelElement;
    int m_nParentIndex;
    bool m_bIsInXSD;

private:
};

class CElementArray : public CIArrayOf<CElement>, public InterfaceImpl, public CXSDNodeBase
{
    friend class CElement;
public:
    CElementArray(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot = NULL) : CXSDNodeBase::CXSDNodeBase(pParentNode, XSD_ELEMENT_ARRAY),\
        m_pSchemaRoot(pSchemaRoot), m_nCountOfElementsInXSD(0)
    {
    }

    virtual ~CElementArray()
    {
    }

    virtual const CXSDNodeBase* getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const;
    virtual const CXSDNodeBase* getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const;

    const CElement* getElementByNameAscending(const char *pName) const;
    const CElement* getElementByNameDescending(const char *pName) const;
    virtual void dump(std::ostream &cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML2(StringBuffer &strQML, int idx = -1) const;
    virtual void getQML3(StringBuffer &strQML, int idx = -1) const;
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);
    virtual const char* getXML(const char* /*pComponent*/);
    virtual int getCountOfSiblingElements(const char *pXPath) const;
    bool anyElementsHaveMaxOccursGreaterThanOne() const;

    virtual void setSchemaRoot(const IPropertyTree *pSchemaRoot)
    {
        assert(m_pSchemaRoot == NULL);
        assert(pSchemaRoot);

        m_pSchemaRoot = pSchemaRoot;
    }
    const IPropertyTree* getSchemaRoot() const
    {
        return m_pSchemaRoot;
    }
    int getCountOfElementsInXSD() const
    {
        return m_nCountOfElementsInXSD;
    }
    void incCountOfElementsInXSD()
    {
        m_nCountOfElementsInXSD++;
    }

    static CElementArray* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = DEFAULT_ELEMENT_ARRAY_XPATH);
    static CElementArray* load(const char *pSchemaFile);

protected:

    const IPropertyTree *m_pSchemaRoot;
    int m_nCountOfElementsInXSD;
    int getSiblingIndex(const char* pXSDXPath, const CElement* pElement);

private:

    CElementArray() : CXSDNodeBase::CXSDNodeBase(NULL, XSD_ELEMENT_ARRAY)
    {
    }
};

#endif // _SCHEMA_ELEMENT_HPP_
