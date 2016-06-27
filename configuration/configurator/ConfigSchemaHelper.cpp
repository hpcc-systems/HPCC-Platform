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

#include "jptree.hpp"
#include "XMLTags.h"
#include <cstring>
#include "jfile.hpp"

#include "ConfigSchemaHelper.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaElement.hpp"
#include "SchemaEnumeration.hpp"
#include "ExceptionStrings.hpp"
#include "BuildSet.hpp"
#include "SchemaMapManager.hpp"
#include "ConfigSchemaHelper.hpp"
#include "ConfigFileUtils.hpp"
#include "JSONMarkUp.hpp"


#define SUCCESS 0
#define FAILURE 1
#define LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET \
int nComponentCount = CBuildSetManager::getInstance()->getBuildSetComponentCount();         \
\
for (int idx = 0; idx < nComponentCount; idx++)

using namespace CONFIGURATOR;



CConfigSchemaHelper* CConfigSchemaHelper::s_pCConfigSchemaHelper = NULL;

CConfigSchemaHelper* CConfigSchemaHelper::getInstance(const char* pDefaultDirOverride)
{
    // not thread safe!!!
    if (s_pCConfigSchemaHelper == NULL)
    {
        s_pCConfigSchemaHelper = new CConfigSchemaHelper();
        s_pCConfigSchemaHelper->m_nTables = 0;

        if (pDefaultDirOverride != NULL && pDefaultDirOverride[0] != 0)
            s_pCConfigSchemaHelper->setBasePath(pDefaultDirOverride);
    }
    return s_pCConfigSchemaHelper;
}

CConfigSchemaHelper* CConfigSchemaHelper::getInstance(const char* pBuildSetFileName, const char *pBaseDirectory, const char *pDefaultDirOverride)
{
    assert(pBuildSetFileName != NULL);
    assert(pBaseDirectory != NULL);

    if (s_pCConfigSchemaHelper == NULL && pBuildSetFileName != NULL && pBaseDirectory != NULL)
    {
       s_pCConfigSchemaHelper = new CConfigSchemaHelper(pBuildSetFileName, pBaseDirectory, pDefaultDirOverride);
       s_pCConfigSchemaHelper->m_nTables = 0;
    }
    return s_pCConfigSchemaHelper;
}

CConfigSchemaHelper::CConfigSchemaHelper(const char* pBuildSetFile, const char* pBuildSetDir, const char* pDefaultDirOverride) : m_pBasePath(NULL), m_nTables(0),\
    m_pEnvPropertyTree(NULL), m_pSchemaMapManager(NULL)
{
    assert(pBuildSetFile != NULL);
    assert(pBuildSetDir != NULL);

    CBuildSetManager::getInstance(pBuildSetFile, pBuildSetDir);
    m_pSchemaMapManager = new CSchemaMapManager();
}

CConfigSchemaHelper::~CConfigSchemaHelper()
{
    delete[] m_pBasePath;
    delete CConfigSchemaHelper::m_pSchemaMapManager;
    CConfigSchemaHelper::m_pSchemaMapManager = NULL;
    CConfigSchemaHelper::s_pCConfigSchemaHelper = NULL;
}

bool CConfigSchemaHelper::populateSchema()
{
    assert(m_pSchemaMapManager != NULL);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != NULL)
        {
            CXSDNodeBase *pNull = NULL;
            CSchema *pSchema = CSchema::load(pSchemaName, pNull);

            assert(pSchema->getLinkCount() == 1);
            m_pSchemaMapManager->setSchemaForXSD(pSchemaName, pSchema);
        }
    }
    populateEnvXPath();

    return true;
}

void CConfigSchemaHelper::printConfigSchema(StringBuffer &strXML) const
{
    assert(m_pSchemaMapManager != NULL);

    const char *pComponent = NULL;
    CSchema* pSchema = NULL;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pComponent == NULL || strcmp(pComponent, pSchemaName) == 0)
        {
            const char* pXSDSchema = pSchemaName;

            if (pXSDSchema == NULL)
                continue;

            pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);

            if (pSchema != NULL)
            {
                if (strXML.length() > 0 ? strcmp(strXML.str(), pXSDSchema) == 0 : true)
                    pSchema->dump(::std::cout);
            }
        }
    }
}

const char* CConfigSchemaHelper::printDocumentation(const char* comp)
{
    assert(comp != NULL && *comp != 0);
    assert(m_pSchemaMapManager != NULL);

    if (comp == NULL || *comp == 0)
        return NULL;

    CSchema* pSchema = NULL;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != NULL && strcmp(comp, pSchemaName) == 0)
        {
             pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);

             assert(pSchema != NULL);

             if (pSchema != NULL)
             {
                static StringBuffer strDoc;
                strDoc.clear(); // needed when printing more than 1 component
                pSchema->getDocumentation(strDoc);

                return strDoc.str();
             }
        }
    }
    return NULL;
}
void CConfigSchemaHelper::printJSON(const char* comp, char **pOutput, int nIdx, bool bCleanUp) const
{
    if (! (comp != NULL && *comp != 0) )
    {
        DBGLOG("no component selected for JSON, index = %d", nIdx);
        return;
    }
    assert(m_pSchemaMapManager != NULL);

    StringBuffer strJSON;

    strJSON.clear();
    resetTables();

    CSchema* pSchema = NULL;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);
        //const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentName(idx);

        if (pSchemaName != NULL && strcmp(comp, pSchemaName) == 0)
        {
            pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);
            assert(pSchema != NULL);

            if (pSchema != NULL)
            {
                pSchema->getJSON(strJSON, 0, nIdx);
                *pOutput = (char*)malloc((sizeof(char))* (strJSON.length())+1);

                if (bCleanUp == true)
                {
                    this->clearLF(strJSON);
                    strJSON.replaceString("\\","\\\\");
                }

                sprintf(*pOutput,"%s",strJSON.str());

                return;
            }
            else
            {
                *pOutput = NULL;
            }
        }
    }
}

void CConfigSchemaHelper::printJSONByKey(const char* key, char **pOutput, bool bCleanUp) const
{
    if (! (key != NULL && *key != 0) )
    {
        DBGLOG("no component key provided for to generate JSON");
        return;
    }

    assert(m_pSchemaMapManager != NULL);

    if (key[0] == '#')
        key = &(key[1]);

    StringBuffer strKey(key);
    StringBuffer strJSON;

    strJSON.clear();
    resetTables();

    const char *pChar = strrchr(key,'[');
    assert(pChar != NULL && strlen(pChar) >= 3);

    int length = strlen(pChar);

    StringBuffer strIdx;

    pChar++;

    int nTemp = 0;

    do
    {
        strIdx.append(*(pChar+nTemp));
        nTemp++;
    }while (*(pChar+nTemp) != ']');

    int nIndexForJSON = atoi(strIdx.str());

    strKey.setLength(strKey.length()-length);  // remove [N] from XPath;

    CSchema* pSchema = NULL;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pProcessName = CBuildSetManager::getInstance()->getBuildSetProcessName(idx);

        if (pProcessName != NULL && strcmp(strKey.str(), pProcessName) == 0)
        {
            pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));
            assert(pSchema != NULL);

            if (pSchema != NULL)
            {
                pSchema->getJSON(strJSON, 0, nIndexForJSON-1);
                *pOutput = (char*)malloc((sizeof(char))* (strJSON.length())+1);

                if (bCleanUp == true)
                {
                    this->clearLF(strJSON);
                    strJSON.replaceString("\\","\\\\");
                }

                sprintf(*pOutput,"%s",strJSON.str());

                return;
            }
            else
            {
                *pOutput = NULL;
            }
        }
    }
}

void CConfigSchemaHelper::printNavigatorJSON(char **pOutput, bool bCleanUp) const
{
    StringBuffer strJSON;

    CJSONMarkUpHelper::getNavigatorJSON(strJSON);

    if (strJSON.length() == 0)
        *pOutput = NULL;

    *pOutput = (char*)malloc((sizeof(char))* (strJSON.length())+1);

    if (bCleanUp == true)
    {
        this->clearLF(strJSON);
        strJSON.replaceString("\\","\\\\");
    }

    sprintf(*pOutput,"%s",strJSON.str());

    return;
}

void CConfigSchemaHelper::printDump(const char* comp) const
{
    assert(comp != NULL && *comp != 0);
    assert(m_pSchemaMapManager != NULL);

    if (comp == NULL || *comp == 0)
        return;

    CSchema* pSchema = NULL;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != NULL && strcmp(comp, pSchemaName) == 0)
        {
             pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);
             assert(pSchema != NULL);

             if (pSchema != NULL)
                pSchema->dump(::std::cout);
        }
    }
}

//test purposes
bool CConfigSchemaHelper::getXMLFromSchema(StringBuffer& strXML, const char* pComponent)
{
    assert (m_pSchemaMapManager != NULL);

    CAttributeArray *pAttributeArray = NULL;
    CElementArray *pElementArray = NULL;
    CSchema* pSchema = NULL;

    strXML.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Environment>\n\t<Software>");

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pComponent == NULL || strcmp(pComponent, pSchemaName) == 0)
        {
            if (pSchemaName == NULL)
                continue;

            pSchema =  m_pSchemaMapManager->getSchemaForXSD(pSchemaName);

            if (pSchema != NULL)
                strXML.append(pSchema->getXML(NULL));
        }
    }
    strXML.append("\t</Software>\n</Environment>\n");

    return true;
}

void CConfigSchemaHelper::addExtensionToBeProcessed(CExtension *pExtension)
{
    assert(pExtension != NULL);
    if (pExtension != NULL)
        m_extensionArr.append(*pExtension);
}

void CConfigSchemaHelper::addAttributeGroupToBeProcessed(CAttributeGroup *pAttributeGroup)
{
    assert(pAttributeGroup != NULL);
    if (pAttributeGroup != NULL)
        m_attributeGroupArr.append(*pAttributeGroup);
}

void CConfigSchemaHelper::addNodeForTypeProcessing(CXSDNodeWithType *pNode)
{
    assert(pNode != NULL);
    if (pNode != NULL)
        m_nodeWithTypeArr.append(*pNode);
}

void CConfigSchemaHelper::addNodeForBaseProcessing(CXSDNodeWithBase *pNode)
{
    assert(pNode != NULL);
    if (pNode != NULL)
        m_nodeWithBaseArr.append(*pNode);
}

void CConfigSchemaHelper::processExtensionArr()
{
    int length = m_extensionArr.length();

    for (int idx = 0; idx < length; idx++)
    {
        CExtension &Extension = (m_extensionArr.item(idx));
        const char *pName = Extension.getBase();

        assert(pName != NULL);

        if (pName != NULL)
        {
            CXSDNode *pNodeBase = NULL;
            pNodeBase = m_pSchemaMapManager->getSimpleTypeWithName(pName) != NULL ? dynamic_cast<CSimpleType*>(m_pSchemaMapManager->getSimpleTypeWithName(pName)) : NULL;

            if (pNodeBase == NULL)
                pNodeBase = m_pSchemaMapManager->getComplexTypeWithName(pName) != NULL ? dynamic_cast<CComplexType*>(m_pSchemaMapManager->getComplexTypeWithName(pName)) : NULL ;

            assert(pNodeBase != NULL);

            if (pNodeBase != NULL)
                Extension.setBaseNode(pNodeBase);
        }
    }
    m_extensionArr.popAll(false);
}

void CConfigSchemaHelper::processAttributeGroupArr()
{
    aindex_t length = m_attributeGroupArr.length();

    for (aindex_t idx = 0; idx < length; idx++)
    {
        CAttributeGroup &AttributeGroup = (m_attributeGroupArr.item(idx));
        const char *pRef = AttributeGroup.getRef();

        assert(pRef != NULL && pRef[0] != 0);
        if (pRef != NULL && pRef[0] != 0)
        {
            assert(m_pSchemaMapManager != NULL);
            CAttributeGroup *pAttributeGroup = m_pSchemaMapManager->getAttributeGroupFromXPath(pRef);

            assert(pAttributeGroup != NULL);
            if (pAttributeGroup != NULL)
                AttributeGroup.setRefNode(pAttributeGroup);
        }
    }
    m_attributeGroupArr.popAll(true);
}

void CConfigSchemaHelper::processNodeWithTypeArr(CXSDNodeBase *pParentNode)
{
    int length = m_nodeWithTypeArr.length();

    for (int idx = 0; idx < length; idx++)
    {
        CXSDNodeWithType *pNodeWithType = &(m_nodeWithTypeArr.item(idx));
        const char *pTypeName = pNodeWithType->getType();

        assert(pTypeName != NULL);
        if (pTypeName != NULL)
        {
            CXSDNode *pNode = NULL;
            pNode = m_pSchemaMapManager->getSimpleTypeWithName(pTypeName) != NULL ? dynamic_cast<CSimpleType*>(m_pSchemaMapManager->getSimpleTypeWithName(pTypeName)) : NULL;

            if (pNode == NULL)
                pNode = m_pSchemaMapManager->getComplexTypeWithName(pTypeName) != NULL ? dynamic_cast<CComplexType*>(m_pSchemaMapManager->getComplexTypeWithName(pTypeName)) : NULL;
            if (pNode == NULL)
                pNode = CXSDBuiltInDataType::create(pNodeWithType, pTypeName);
            if (pNode != NULL)
                pNodeWithType->setTypeNode(pNode);
            else
                PROGLOG("Unsupported type '%s'", pTypeName);
        }
    }
    m_nodeWithTypeArr.popAll(true);
}

void CConfigSchemaHelper::processNodeWithBaseArr()
{
    int length = m_nodeWithBaseArr.length();

    for (int idx = 0; idx < length; idx++)
    {
        CXSDNodeWithBase *pNodeWithBase = &(this->m_nodeWithBaseArr.item(idx));
        const char *pBaseName = pNodeWithBase->getBase();

        assert(pBaseName != NULL);
        if (pBaseName != NULL)
        {
            CXSDNode *pNode = NULL;
            pNode = m_pSchemaMapManager->getSimpleTypeWithName(pBaseName) != NULL ? dynamic_cast<CSimpleType*>(m_pSchemaMapManager->getSimpleTypeWithName(pBaseName)) : NULL;

            if (pNode == NULL)
                pNode = m_pSchemaMapManager->getComplexTypeWithName(pBaseName) != NULL ? dynamic_cast<CComplexType*>(m_pSchemaMapManager->getComplexTypeWithName(pBaseName)) : NULL;
            if (pNode == NULL)
                pNode = CXSDBuiltInDataType::create(pNode, pBaseName);

            assert(pNode != NULL);
            if (pNode != NULL)
                pNodeWithBase->setBaseNode(pNode);
            else
                PROGLOG("Unsupported type '%s'", pBaseName);
        }
    }
    m_nodeWithBaseArr.popAll(false);
}

void CConfigSchemaHelper::addElementForRefProcessing(CElement *pElement)
{
    assert (pElement != NULL);
    if (pElement != NULL)
        m_ElementArr.append(*pElement);
}

void CConfigSchemaHelper::processElementArr(CElement *pElement)
{
    int length = m_nodeWithBaseArr.length();

    for (int idx = 0; idx < length; idx++)
    {
        CElement *pElement = &(this->m_ElementArr.item(idx));
        const char *pRef = pElement->getRef();

        assert(pRef != NULL);
        if (pRef != NULL)
        {
            CElement *pRefElementNode = NULL;
            pRefElementNode = m_pSchemaMapManager->getElementWithName(pRef);

            if (pRefElementNode != NULL)
                pElement->setRefElementNode(pRefElementNode);
            else
                //TODO: throw exception
                assert(!"Unknown element referenced");
        }
    }
    m_ElementArr.popAll(false);
}

void CConfigSchemaHelper::populateEnvXPath()
{
    CSchema* pSchema = NULL;
    StringBuffer strXPath;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));

        if (pSchema != NULL)
            pSchema->populateEnvXPath(strXPath);
    }
}

void CConfigSchemaHelper::loadEnvFromConfig(const char *pEnvFile)
{
    assert(pEnvFile != NULL);

    typedef ::IPropertyTree PT;
    Linked<PT> pEnvXMLRoot;

    try
    {
        pEnvXMLRoot.setown(createPTreeFromXMLFile(pEnvFile));
    }
    catch (...)
    {
        CONFIGURATOR::MakeExceptionFromMap(EX_STR_CAN_NOT_PROCESS_ENV_XML);
    }

    CSchema* pSchema = NULL;

    this->setEnvPropertyTree(pEnvXMLRoot.getLink());
    this->setEnvFilePath(pEnvFile);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));

        if (pSchema != NULL)
            pSchema->loadXMLFromEnvXml(pEnvXMLRoot);
    }
}

void CConfigSchemaHelper::addToolTip(const char *js)
{
    assert (js != NULL);
    assert (js[0] != 0);

    if (js == NULL || js[0] == 0)
        return;

    m_strToolTipsJS.append(js);
}

const char* CConfigSchemaHelper::getToolTipJS() const
{
    static StringBuffer strJS;
    strJS.clear();

    for (int idx = 0; idx < m_strToolTipsJS.length(); idx++)
    {
        strJS.append(m_strToolTipsJS.item(idx));
    }
    return strJS.str();
}

void CConfigSchemaHelper::setEnvTreeProp(const char *pXPath, const char* pValue)
{
    assert(pXPath != NULL && pXPath[0] != 0);
    assert(m_pSchemaMapManager != NULL);

    CAttribute *pAttribute = m_pSchemaMapManager->getAttributeFromXPath(pXPath);
    assert(pAttribute != NULL);

    StringBuffer strPropName("@");
    strPropName.append(pAttribute->getName());

    if (this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->queryProp(strPropName.str()) == NULL)
        //should check if this attribute is optional for validation
        this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->setProp(strPropName.str(), pValue);
    else if (strcmp (this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->queryProp(strPropName.str()), pValue) == 0)
        return; // nothing changed
    else
        this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->setProp(strPropName.str(), pValue);
}

const char* CConfigSchemaHelper::getTableValue(const char* pXPath,  int nRow) const
{
    assert(pXPath != NULL);
    assert(m_pSchemaMapManager != NULL);

    CAttribute *pAttribute = m_pSchemaMapManager->getAttributeFromXPath(pXPath);
    CElement *pElement = NULL;

    if (pAttribute == NULL)
    {
        pElement = m_pSchemaMapManager->getElementFromXPath(pXPath);
        assert(pElement != NULL);
        return pElement->getEnvValueFromXML();
    }
    else
    {
        assert(pAttribute != NULL);
        if (nRow == 1)
            return pAttribute->getEnvValueFromXML();
        else
        {
            StringBuffer strXPath(pXPath);
            StringBuffer strXPathOrignal(pXPath);

            CConfigSchemaHelper::stripXPathIndex(strXPath);

            strXPath.appendf("[%d]", nRow);
            char pTemp[64];
            int offset = strXPath.length() - (strlen(itoa(nRow, pTemp, 10)) - 1);

            strXPath.append(strXPathOrignal, strXPath.length(), strXPathOrignal.length()-offset);

            pAttribute = m_pSchemaMapManager->getAttributeFromXPath(strXPath.str());

            if (pAttribute == NULL)
                return NULL;

            return pAttribute->getEnvValueFromXML();
        }
    }
}

int CConfigSchemaHelper::getElementArraySize(const char *pXPath) const
{
    assert(pXPath != NULL);
    assert(m_pSchemaMapManager != NULL);

    //CElementArray *pElementArray = m_pSchemaMapManager->getElementArrayFromXSDXPath(pXPath);
    CElementArray *pElementArray = m_pSchemaMapManager->getElementArrayFromXPath(pXPath);

    if (pElementArray == NULL)
        return 0;

    //return pElementArray->getCountOfSiblingElements(pXPath);
    VStringBuffer strXPath("%s[1]",pElementArray->getXSDXPath());
    //return pElementArray->getCountOfSiblingElements(pElementArray->getXSDXPath());
    return pElementArray->getCountOfSiblingElements(strXPath.str());
}

const char* CConfigSchemaHelper::getAttributeXSDXPathFromEnvXPath(const char* pEnvXPath) const
{
    assert(pEnvXPath != NULL && *pEnvXPath != 0);
    assert(m_pSchemaMapManager != NULL);
    CAttribute *pAttribute = m_pSchemaMapManager->getAttributeFromXPath(pEnvXPath);

    assert(pAttribute != NULL);
    return pAttribute->getXSDXPath();
}

const char* CConfigSchemaHelper::getElementArrayXSDXPathFromEnvXPath(const char* pXSDXPath) const
{
    assert(pXSDXPath != NULL);
    assert(m_pSchemaMapManager != NULL);
    CElementArray *pElementArray = m_pSchemaMapManager->getElementArrayFromXSDXPath(pXSDXPath);

    assert(pElementArray != NULL);
    return pElementArray->getXSDXPath();
}

void CConfigSchemaHelper::appendAttributeXPath(const char* pXPath)
{
    m_strArrayEnvXPaths.append(pXPath);
}

void CConfigSchemaHelper::appendElementXPath(const char* pXPath)
{
    m_strArrayEnvXPaths.append(pXPath);
}

int CConfigSchemaHelper::stripXPathIndex(StringBuffer &strXPath)
{
    int nLen = strXPath.length()-3;
    int nLengthOfStringInBracket = 3;

    while (nLen > 0)
    {
        if (strXPath[nLen] == '[')
        {
            strXPath.reverse().remove(0,strXPath.length()-nLen).reverse();
            return nLengthOfStringInBracket;
        }
        nLen--;
        nLengthOfStringInBracket++;
    }
    return nLengthOfStringInBracket;
}

bool CConfigSchemaHelper::isXPathTailAttribute(const StringBuffer &strXPath)
{
    int nLen = strXPath.length()-3;

    while (nLen > 0)
    {
        if (strXPath[nLen] == '[')
        {
            if (strXPath[nLen+1] == '@')
                return true;
            else
                return false;
        }
        nLen--;
    }
    assert(!"Control should not reach here");
    return false;
}

void CConfigSchemaHelper::setBasePath(const char *pBasePath)
{
    assert(m_pBasePath == NULL);
    int nLength = strlen(pBasePath);
    m_pBasePath = new char[nLength+1];
    strcpy(m_pBasePath, pBasePath);
}


bool CConfigSchemaHelper::saveConfigurationFile() const
{
    assert(m_strEnvFilePath.length() != 0);

    if (m_strEnvFilePath.length() == 0)
        return false;

    if (this->getConstEnvPropertyTree() == NULL)
        return false;

    StringBuffer strXML;
    strXML.appendf("<" XML_HEADER ">\n<!-- Edited with THE CONFIGURATOR -->\n");
    ::toXML(this->getConstEnvPropertyTree(), strXML, 0, XML_SortTags | XML_Format);

    if (CConfigFileUtils::getInstance()->writeConfigurationToFile(m_strEnvFilePath.str(), strXML.str(), strXML.length()) == CConfigFileUtils::CF_NO_ERROR)
        return true;
    else
        return false;
}

bool CConfigSchemaHelper::saveConfigurationFileAs(const char *pFilePath)
{
    assert(pFilePath && *pFilePath);

    if (pFilePath == NULL || *pFilePath == 0)
        return false;

    if (this->getConstEnvPropertyTree() == NULL)
        return false;

    StringBuffer strXML;
    strXML.appendf("<" XML_HEADER ">\n<!-- Edited with THE CONFIGURATOR -->\n");
    ::toXML(this->getConstEnvPropertyTree(), strXML, 0, XML_SortTags | XML_Format);

    if (CConfigFileUtils::getInstance()->writeConfigurationToFile(pFilePath, strXML.str(), strXML.length()) == CConfigFileUtils::CF_NO_ERROR)
    {
        m_strEnvFilePath.set(pFilePath);
        return true;
    }
    else
        return false;
}

void CConfigSchemaHelper::addKeyRefForReverseAssociation(const CKeyRef *pKeyRef) const
{
}

void CConfigSchemaHelper::processKeyRefReverseAssociation() const
{
}

void CConfigSchemaHelper::addKeyForReverseAssociation(const CKey *pKeyRef) const
{
}

void CConfigSchemaHelper::processKeyReverseAssociation() const
{
}

int CConfigSchemaHelper::getInstancesOfComponentType(const char *pCompType) const
{
    assert(pCompType != NULL && *pCompType != 0);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pCompName = CBuildSetManager::getInstance()->getBuildSetComponentTypeName(idx);//  ->getBuildSetProcessName(idx);
        const char *pProcessName = CBuildSetManager::getInstance()->getBuildSetProcessName(idx);

        if (pCompName != NULL && strcmp(pCompName, pCompType) == 0)
        {
             CSchema *pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));

             assert(pSchema != NULL);

             if (pSchema == NULL)
                 return FAILURE;

             int nCount = 0;
             ::VStringBuffer strXPath("./%s/%s[%d]", XML_TAG_SOFTWARE, pProcessName, 1);

             while (true)
             {
                 ::IPropertyTree *pTree = CConfigSchemaHelper::getInstance()->getEnvPropertyTree();

                 if (pTree == NULL)
                     return FAILURE;

                 if (pTree->queryPropTree(strXPath.str()) == NULL)
                     return nCount;

                 nCount++;
                 strXPath.setf("./%s/%s[%d]", XML_TAG_SOFTWARE, pProcessName, nCount+1);
             }
        }
    }
    return SUCCESS;
}

const char* CConfigSchemaHelper::getInstanceNameOfComponentType(const char *pCompType, int idx)
{
    if (pCompType == NULL || *pCompType == 0)
        return NULL;  // throw exception?
    if (this->getEnvPropertyTree() == NULL)
        return NULL;  // throw exception?

    ::VStringBuffer strXPath("./%s/%s[%d]", XML_TAG_SOFTWARE, pCompType, idx+1);

    typedef ::IPropertyTree jlibIPropertyTree;
    const ::IPropertyTree *pTree = const_cast<const jlibIPropertyTree*>(this->getEnvPropertyTree()->queryPropTree(strXPath.str()));

    return pTree->queryProp(XML_ATTR_NAME);
}

void CConfigSchemaHelper::clearLF(::StringBuffer& strToClear)
{
    strToClear.replaceString("\n","");
}

CConfigSchemaHelper* CConfigSchemaHelper::getNewInstance(const char* pDefaultDirOverride)
{
    if (CConfigSchemaHelper::s_pCConfigSchemaHelper != NULL)
    {
        delete CConfigSchemaHelper::s_pCConfigSchemaHelper;
        CConfigSchemaHelper::s_pCConfigSchemaHelper = NULL;
    }

    CConfigSchemaHelper::getInstance(pDefaultDirOverride);
    CConfigSchemaHelper::getInstance()->populateSchema();

    return CConfigSchemaHelper::getInstance();
}
