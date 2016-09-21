/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

CConfigSchemaHelper* CConfigSchemaHelper::s_pCConfigSchemaHelper = nullptr;

CConfigSchemaHelper* CConfigSchemaHelper::getInstance(const char* pDefaultDirOverride)
{
    // not thread safe!!!
    if (s_pCConfigSchemaHelper == nullptr)
    {
        s_pCConfigSchemaHelper = new CConfigSchemaHelper();
        s_pCConfigSchemaHelper->m_nTables = 0;

        if (pDefaultDirOverride != nullptr && pDefaultDirOverride[0] != 0)
            s_pCConfigSchemaHelper->setBasePath(pDefaultDirOverride);
    }
    return s_pCConfigSchemaHelper;
}

CConfigSchemaHelper* CConfigSchemaHelper::getInstance(const char* pBuildSetFileName, const char *pBaseDirectory, const char *pDefaultDirOverride)
{
    assert(pBuildSetFileName != nullptr);
    assert(pBaseDirectory != nullptr);

    if (s_pCConfigSchemaHelper == nullptr && pBuildSetFileName != nullptr && pBaseDirectory != nullptr)
    {
       s_pCConfigSchemaHelper = new CConfigSchemaHelper(pBuildSetFileName, pBaseDirectory, pDefaultDirOverride);
       s_pCConfigSchemaHelper->m_nTables = 0;
    }
    return s_pCConfigSchemaHelper;
}

CConfigSchemaHelper::CConfigSchemaHelper(const char* pBuildSetFile, const char* pBuildSetDir, const char* pDefaultDirOverride) : m_pBasePath(nullptr), m_nTables(0),\
    m_pEnvPropertyTree(nullptr), m_pSchemaMapManager(nullptr)
{
    assert(pBuildSetFile != nullptr);
    assert(pBuildSetDir != nullptr);

    CBuildSetManager::getInstance(pBuildSetFile, pBuildSetDir);
    m_pSchemaMapManager = new CSchemaMapManager();
}

CConfigSchemaHelper::~CConfigSchemaHelper()
{
    delete[] m_pBasePath;
    delete CConfigSchemaHelper::m_pSchemaMapManager;
    CConfigSchemaHelper::m_pSchemaMapManager = nullptr;
    CConfigSchemaHelper::s_pCConfigSchemaHelper = nullptr;
}

bool CConfigSchemaHelper::populateSchema()
{
    assert(m_pSchemaMapManager != nullptr);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != nullptr)
        {
            CXSDNodeBase *pNodeBase = nullptr;
            CSchema *pSchema = CSchema::load(pSchemaName, pNodeBase);

            assert(pSchema->getLinkCount() == 1);
            m_pSchemaMapManager->setSchemaForXSD(pSchemaName, pSchema);
        }
    }
    populateEnvXPath();

    return true;
}

void CConfigSchemaHelper::printConfigSchema(StringBuffer &strXML) const
{
    assert(m_pSchemaMapManager != nullptr);

    const char *pComponent = nullptr;
    CSchema* pSchema = nullptr;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pComponent == nullptr || strcmp(pComponent, pSchemaName) == 0)
        {
            if (pSchemaName == nullptr)
                continue;

            pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);

            if (pSchema != nullptr)
            {
                if (strXML.length() > 0 ? strcmp(strXML.str(), pSchemaName) == 0 : true)
                    pSchema->dump(::std::cout);
            }
        }
    }
}

void CConfigSchemaHelper::printDocumentation(const char* comp, char **pOutput) const
{
    assert(comp != nullptr && *comp != 0);
    assert(m_pSchemaMapManager != nullptr);

    if (!comp || !*comp)
        return;

    CSchema* pSchema = nullptr;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != nullptr && strcmp(comp, pSchemaName) == 0)
        {
             pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);

             assert(pSchema != nullptr);

             if (pSchema != nullptr)
             {
                StringBuffer strDoc;
                pSchema->getDocumentation(strDoc);

                *pOutput = new char[strDoc.length()+1];
                sprintf(*pOutput,"%s",strDoc.str());
                return;
             }
        }
    }
    *pOutput = nullptr;
}

void CConfigSchemaHelper::printJSON(const char* comp, char **pOutput, int nIdx, bool bCleanUp) const
{
    if (! (comp != nullptr && *comp != 0) )
    {
        DBGLOG("no component selected for JSON, index = %d", nIdx);
        return;
    }
    assert(m_pSchemaMapManager != nullptr);

    StringBuffer strJSON;

    strJSON.clear();
    resetTables();

    CSchema* pSchema = nullptr;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != nullptr && strcmp(comp, pSchemaName) == 0)
        {
            pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);
            assert(pSchema != nullptr);

            if (pSchema != nullptr)
            {
                pSchema->getJSON(strJSON, 0, nIdx);
                *pOutput = new char[strJSON.length()+1];

                if (bCleanUp == true)
                {
                    this->clearLF(strJSON);
                    strJSON.replaceString("\\","\\\\");
                }

                sprintf(*pOutput,"%s",strJSON.str());
                return;
            }
            else
                *pOutput = nullptr;
        }
    }
}

void CConfigSchemaHelper::printJSONByKey(const char* key, char **pOutput, bool bCleanUp) const
{
    if ( !key || !*key)
    {
        DBGLOG("no component key provided for to generate JSON");
        return;
    }

    assert(m_pSchemaMapManager != nullptr);

    if (key[0] == '#')
        key = &(key[1]);

    StringBuffer strKey(key);
    StringBuffer strJSON;

    resetTables();

    const char *pChar = strrchr(key,'[');
    assert(pChar != nullptr);
    int length = strlen(pChar);
    assert(length >= 3);

    StringBuffer strIdx;

    pChar++;

    do
    {
        strIdx.append(*pChar);
        pChar++;
    } while (*pChar != 0 && *pChar != ']');


    int nIndexForJSON = atoi(strIdx.str());

    strKey.setLength(strKey.length()-length);  // remove [N] from XPath;

    CSchema* pSchema = nullptr;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pProcessName = CBuildSetManager::getInstance()->getBuildSetProcessName(idx);

        if (pProcessName != nullptr && strcmp(strKey.str(), pProcessName) == 0)
        {
            pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));
            assert(pSchema != nullptr);

            if (pSchema != nullptr)
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
                *pOutput = nullptr;
        }
    }
}

void CConfigSchemaHelper::printNavigatorJSON(char **pOutput, bool bCleanUp) const
{
    StringBuffer strJSON;

    CJSONMarkUpHelper::getNavigatorJSON(strJSON);

    if (strJSON.length() == 0)
        *pOutput = nullptr;

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
    assert(comp != nullptr && *comp != 0);
    assert(m_pSchemaMapManager != nullptr);

    if (comp == nullptr || *comp == 0)
        return;

    CSchema* pSchema = nullptr;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pSchemaName != nullptr && strcmp(comp, pSchemaName) == 0)
        {
             pSchema = m_pSchemaMapManager->getSchemaForXSD(pSchemaName);
             assert(pSchema != nullptr);

             if (pSchema != nullptr)
                pSchema->dump(::std::cout);
        }
    }
}

//test purposes
bool CConfigSchemaHelper::getXMLFromSchema(StringBuffer& strXML, const char* pComponent)
{
    assert (m_pSchemaMapManager != nullptr);

    CSchema* pSchema = nullptr;

    strXML.append(CONFIGURATOR_HEADER);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pSchemaName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);

        if (pComponent == nullptr || strcmp(pComponent, pSchemaName) == 0)
        {
            if (pSchemaName == nullptr)
                continue;

            pSchema =  m_pSchemaMapManager->getSchemaForXSD(pSchemaName);

            if (pSchema != nullptr)
                strXML.append(pSchema->getXML(nullptr));
        }
    }
    strXML.append("\t</Software>\n</Environment>\n");

    return true;
}

void CConfigSchemaHelper::addExtensionToBeProcessed(CExtension *pExtension)
{
    assert(pExtension != nullptr);
    if (pExtension != nullptr)
        m_extensionArr.append(*pExtension);
}

void CConfigSchemaHelper::addAttributeGroupToBeProcessed(CAttributeGroup *pAttributeGroup)
{
    assert(pAttributeGroup != nullptr);
    if (pAttributeGroup != nullptr)
        m_attributeGroupArr.append(*pAttributeGroup);
}

void CConfigSchemaHelper::addNodeForTypeProcessing(CXSDNodeWithType *pNode)
{
    assert(pNode != nullptr);
    if (pNode != nullptr)
        m_nodeWithTypeArr.append(*pNode);
}

void CConfigSchemaHelper::addNodeForBaseProcessing(CXSDNodeWithBase *pNode)
{
    assert(pNode != nullptr);
    if (pNode != nullptr)
        m_nodeWithBaseArr.append(*pNode);
}

void CConfigSchemaHelper::processExtensionArr()
{
    int length = m_extensionArr.length();

    for (int idx = 0; idx < length; idx++)
    {
        CExtension &extension = (m_extensionArr.item(idx));
        const char *pName = extension.getBase();

        assert(pName != nullptr);

        if (pName != nullptr)
        {
            CXSDNode *pNodeBase = nullptr;
            pNodeBase = m_pSchemaMapManager->getSimpleTypeWithName(pName) != nullptr ? dynamic_cast<CSimpleType*>(m_pSchemaMapManager->getSimpleTypeWithName(pName)) : nullptr;

            if (pNodeBase == nullptr)
                pNodeBase = m_pSchemaMapManager->getComplexTypeWithName(pName) != nullptr ? dynamic_cast<CComplexType*>(m_pSchemaMapManager->getComplexTypeWithName(pName)) : nullptr ;

            assert(pNodeBase != nullptr);

            if (pNodeBase != nullptr)
                extension.setBaseNode(pNodeBase);
        }
    }
    m_extensionArr.popAll(false);
}

void CConfigSchemaHelper::processAttributeGroupArr()
{
    aindex_t length = m_attributeGroupArr.length();

    for (aindex_t idx = 0; idx < length; idx++)
    {
        CAttributeGroup &attributeGroup = (m_attributeGroupArr.item(idx));
        const char *pRef = attributeGroup.getRef();

        assert(pRef != nullptr && pRef[0] != 0);
        if (pRef != nullptr && pRef[0] != 0)
        {
            assert(m_pSchemaMapManager != nullptr);
            CAttributeGroup *pAttributeGroup = m_pSchemaMapManager->getAttributeGroupFromXPath(pRef);

            assert(pAttributeGroup != nullptr);
            if (pAttributeGroup != nullptr)
                attributeGroup.setRefNode(pAttributeGroup);
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

        assert(pTypeName != nullptr);
        if (pTypeName != nullptr)
        {
            CXSDNode *pNode = nullptr;
            pNode = m_pSchemaMapManager->getSimpleTypeWithName(pTypeName) != nullptr ? dynamic_cast<CSimpleType*>(m_pSchemaMapManager->getSimpleTypeWithName(pTypeName)) : nullptr;

            if (pNode == nullptr)
                pNode = m_pSchemaMapManager->getComplexTypeWithName(pTypeName) != nullptr ? dynamic_cast<CComplexType*>(m_pSchemaMapManager->getComplexTypeWithName(pTypeName)) : nullptr;
            if (pNode == nullptr)
                pNode = CXSDBuiltInDataType::create(pNodeWithType, pTypeName);
            if (pNode != nullptr)
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

        assert(pBaseName != nullptr);
        if (pBaseName != nullptr)
        {
            CXSDNode *pNode = nullptr;
            pNode = m_pSchemaMapManager->getSimpleTypeWithName(pBaseName) != nullptr ? dynamic_cast<CSimpleType*>(m_pSchemaMapManager->getSimpleTypeWithName(pBaseName)) : nullptr;

            if (pNode == nullptr)
                pNode = m_pSchemaMapManager->getComplexTypeWithName(pBaseName) != nullptr ? dynamic_cast<CComplexType*>(m_pSchemaMapManager->getComplexTypeWithName(pBaseName)) : nullptr;
            if (pNode == nullptr)
                pNode = CXSDBuiltInDataType::create(pNode, pBaseName);

            assert(pNode != nullptr);
            if (pNode != nullptr)
                pNodeWithBase->setBaseNode(pNode);
            else
                PROGLOG("Unsupported type '%s'", pBaseName);
        }
    }
    m_nodeWithBaseArr.popAll(false);
}

void CConfigSchemaHelper::addElementForRefProcessing(CElement *pElement)
{
    assert (pElement != nullptr);
    if (pElement != nullptr)
        m_ElementArr.append(*pElement);
}

void CConfigSchemaHelper::processElementArr()
{
    int length = m_nodeWithBaseArr.length();

    for (int idx = 0; idx < length; idx++)
    {
        CElement *pElement = &(this->m_ElementArr.item(idx));
        const char *pRef = pElement->getRef();

        assert(pRef != nullptr);
        if (pRef != nullptr)
        {
            CElement *pRefElementNode = nullptr;
            pRefElementNode = m_pSchemaMapManager->getElementWithName(pRef);

            if (pRefElementNode != nullptr)
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
    CSchema* pSchema = nullptr;
    StringBuffer strXPath;

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));

        if (pSchema != nullptr)
            pSchema->populateEnvXPath(strXPath);
    }
}

void CConfigSchemaHelper::loadEnvFromConfig(const char *pEnvFile)
{
    assert(pEnvFile != nullptr);

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

    CSchema* pSchema = nullptr;

    this->setEnvPropertyTree(pEnvXMLRoot.getLink());
    this->setEnvFilePath(pEnvFile);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));

        if (pSchema != nullptr)
            pSchema->loadXMLFromEnvXml(pEnvXMLRoot);
    }
}

void CConfigSchemaHelper::setEnvTreeProp(const char *pXPath, const char* pValue)
{
    assert(pXPath != nullptr && pXPath[0] != 0);
    assert(m_pSchemaMapManager != nullptr);

    CAttribute *pAttribute = m_pSchemaMapManager->getAttributeFromXPath(pXPath);
    assert(pAttribute != nullptr);

    StringBuffer strPropName("@");
    strPropName.append(pAttribute->getName());

    if (this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->queryProp(strPropName.str()) == nullptr)
        //should check if this attribute is optional for validation
        this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->setProp(strPropName.str(), pValue);
    else if (strcmp (this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->queryProp(strPropName.str()), pValue) == 0)
        return; // nothing changed
    else
        this->getEnvPropertyTree()->queryPropTree(pAttribute->getConstAncestorNode(1)->getEnvXPath())->setProp(strPropName.str(), pValue);
}

const char* CConfigSchemaHelper::getTableValue(const char* pXPath,  int nRow) const
{
    assert(pXPath != nullptr);
    assert(m_pSchemaMapManager != nullptr);

    CAttribute *pAttribute = m_pSchemaMapManager->getAttributeFromXPath(pXPath);
    CElement *pElement = nullptr;

    if (pAttribute == nullptr)
    {
        pElement = m_pSchemaMapManager->getElementFromXPath(pXPath);
        assert(pElement != nullptr);
        return pElement->getEnvValueFromXML();
    }
    else
    {
        assert(pAttribute != nullptr);
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

            if (pAttribute == nullptr)
                return nullptr;

            return pAttribute->getEnvValueFromXML();
        }
    }
}

int CConfigSchemaHelper::getElementArraySize(const char *pXPath) const
{
    assert(pXPath != nullptr);
    assert(m_pSchemaMapManager != nullptr);

    CElementArray *pElementArray = m_pSchemaMapManager->getElementArrayFromXPath(pXPath);

    if (pElementArray == nullptr)
        return 0;

    VStringBuffer strXPath("%s[1]",pElementArray->getXSDXPath());

    return pElementArray->getCountOfSiblingElements(strXPath.str());
}

const char* CConfigSchemaHelper::getAttributeXSDXPathFromEnvXPath(const char* pEnvXPath) const
{
    assert(pEnvXPath != nullptr && *pEnvXPath != 0);
    assert(m_pSchemaMapManager != nullptr);
    CAttribute *pAttribute = m_pSchemaMapManager->getAttributeFromXPath(pEnvXPath);

    assert(pAttribute != nullptr);
    return pAttribute->getXSDXPath();
}

const char* CConfigSchemaHelper::getElementArrayXSDXPathFromEnvXPath(const char* pXSDXPath) const
{
    assert(pXSDXPath != nullptr);
    assert(m_pSchemaMapManager != nullptr);
    CElementArray *pElementArray = m_pSchemaMapManager->getElementArrayFromXSDXPath(pXSDXPath);

    assert(pElementArray != nullptr);
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

size_t CConfigSchemaHelper::getXPathIndexLength(const char *pXPath)
{
    if (!pXPath || !pXPath)
        return 0;

    size_t length  = strlen(pXPath);

    if (length < 4) // min length must be atleast 4 : T[N]
        return 0;

    const char *pFinger = &(pXPath[length-1]);

    while (pFinger != pXPath && *pFinger != '[')
    {
        pFinger--;
    }
    return (pFinger == pXPath ? 0 : &(pXPath[length]) - pFinger);
}

int CConfigSchemaHelper::stripXPathIndex(StringBuffer &strXPath)
{
    int nStripped = getXPathIndexLength(strXPath.str());
    strXPath.setLength(strXPath.length()-nStripped);
    return nStripped;
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
    assert(m_pBasePath == nullptr);
    int nLength = strlen(pBasePath);
    m_pBasePath = new char[nLength+1];
    strcpy(m_pBasePath, pBasePath);
}


bool CConfigSchemaHelper::saveConfigurationFile() const
{
    assert(m_strEnvFilePath.length() != 0);

    if (m_strEnvFilePath.length() == 0)
        return false;

    if (this->getConstEnvPropertyTree() == nullptr)
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

    if (pFilePath == nullptr || *pFilePath == 0)
        return false;

    if (this->getConstEnvPropertyTree() == nullptr)
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
    assert(pCompType != nullptr && *pCompType != 0);

    LOOP_THRU_BUILD_SET_MANAGER_BUILD_SET
    {
        const char *pCompName = CBuildSetManager::getInstance()->getBuildSetComponentTypeName(idx);//  ->getBuildSetProcessName(idx);
        const char *pProcessName = CBuildSetManager::getInstance()->getBuildSetProcessName(idx);

        if (pCompName != nullptr && strcmp(pCompName, pCompType) == 0)
        {
             CSchema *pSchema = m_pSchemaMapManager->getSchemaForXSD(CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx));

             assert(pSchema != nullptr);

             if (pSchema == nullptr)
                 return FAILURE;

             int nCount = 0;
             ::VStringBuffer strXPath("./%s/%s[%d]", XML_TAG_SOFTWARE, pProcessName, 1);

             while (true)
             {
                 ::IPropertyTree *pTree = CConfigSchemaHelper::getInstance()->getEnvPropertyTree();

                 if (pTree == nullptr)
                     return FAILURE;

                 if (pTree->queryPropTree(strXPath.str()) == nullptr)
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
    if (pCompType == nullptr || *pCompType == 0)
        return nullptr;  // throw exception?
    if (this->getEnvPropertyTree() == nullptr)
        return nullptr;  // throw exception?

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
    if (CConfigSchemaHelper::s_pCConfigSchemaHelper != nullptr)
    {
        delete CConfigSchemaHelper::s_pCConfigSchemaHelper;
        CConfigSchemaHelper::s_pCConfigSchemaHelper = nullptr;
    }

    CConfigSchemaHelper::getInstance(pDefaultDirOverride);
    CConfigSchemaHelper::getInstance()->populateSchema();

    return CConfigSchemaHelper::getInstance();
}
