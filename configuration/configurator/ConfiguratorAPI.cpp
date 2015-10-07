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

#include "ConfiguratorAPI.hpp"
#include "ConfiguratorMain.hpp"
#include "BuildSet.hpp"
#include "ConfigSchemaHelper.hpp"
#include "ConfiguratorMain.hpp"
#include "jstring.hpp"
#include "SchemaMapManager.hpp"
#include "SchemaEnumeration.hpp"
#include "SchemaCommon.hpp"
#include "ConfiguratorMain.hpp"
#include "EnvironmentModel.hpp"
#include "ConfigNotifications.hpp"
#include <iostream>
#include "jlog.hpp"

static int nAllocatedTables = 0;
static  char *modelName[MAX_ARRAY_X];

const char* getTableDataModelName(int index)
{
    if (index < nAllocatedTables)
        return modelName[index];
    else
    {
        modelName[index] = new char[MAX_ARRAY_Y];
        sprintf(modelName[index],"tableDataModel%d", index);

        nAllocatedTables++;
        return modelName[index];
    }
}

void deleteTableModels()
{
    while (nAllocatedTables > 0)
    {
        delete[] modelName[nAllocatedTables];
    }
}

namespace CONFIGURATOR_API
{
static CConfigSchemaHelper *s_pConfigSchemaHelper = NULL;

void reload(const char *pFile)
{
    assert(pFile != NULL && *pFile != 0);
    delete s_pConfigSchemaHelper;

    s_pConfigSchemaHelper = NULL;
    s_pConfigSchemaHelper = CConfigSchemaHelper::getInstance();
    s_pConfigSchemaHelper->populateSchema();

    CConfigSchemaHelper::getInstance()->loadEnvFromConfig(pFile);
}

int getNumberOfAvailableComponents()
{
    assert(s_pConfigSchemaHelper != NULL);
    return CBuildSetManager::getInstance()->getBuildSetComponentCount();
}

int getNumberOfAvailableServices()
{
    assert(s_pConfigSchemaHelper != NULL);
    return  CBuildSetManager::getInstance()->getBuildSetServiceCount();
}

#ifdef CONFIGURATOR_LIB

int initialize()
{
    assert(s_pConfigSchemaHelper == NULL);

    static bool bOnce = true;

    if (bOnce == true)
    {
        bOnce = false;

        InitModuleObjects();

    }

    s_pConfigSchemaHelper = CConfigSchemaHelper::getInstance();
    s_pConfigSchemaHelper->populateSchema();

    return 1;
}

#else // CONFIGURATOR_LIB

int initialize(int argc, char *argv[])
{
    assert(s_pConfigSchemaHelper == NULL);
    InitModuleObjects();

    s_pConfigSchemaHelper = CConfigSchemaHelper::getInstance();

    return 0;
}

#endif // CONFIGURATOR_LIB

int getValue(const char *pXPath, char *pValue)
{
    // By Default, return xPath as value.
    strcpy(pValue, pXPath);
    CAttribute *pAttribute = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getAttributeFromXPath(pXPath);

    if(pAttribute == NULL)
        std::cout << "xPath: " << pXPath << "| value: " << pXPath << std::endl;
    else if (pAttribute->isInstanceValueValid() == true)
    {
        strcpy(pValue, pAttribute->getInstanceValue());
        std::cout << "xPath: " << pXPath << "| value: " << pValue << std::endl;
    }
    return true;
}

void setValue(const char *pXPath, const char *pValue)
{
    assert(pXPath != NULL && pXPath[0] != 0);
    assert(pValue != NULL);
    CAttribute *pAttribute = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getAttributeFromXPath(pXPath);

    assert(pAttribute != NULL);
    pAttribute->setEnvValueFromXML(pValue);

    CConfigSchemaHelper::getInstance()->setEnvTreeProp(pXPath, pValue);
}

int getIndex(const char *pXPath)
{
    CRestriction *pRestriction = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getRestrictionFromXPath(pXPath);
    assert(pRestriction != NULL);
    assert(pRestriction->getEnumerationArray() != NULL);

    return pRestriction->getEnumerationArray()->getEnvValueNodeIndex();
}

void setIndex(const char *pXPath, int newIndex)
{
    assert(newIndex >= 0);

    CRestriction *pRestriction = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getRestrictionFromXPath(pXPath);

    assert(pRestriction != NULL);
    assert(pRestriction->getEnumerationArray() != NULL);
    pRestriction->getEnumerationArray()->setEnvValueNodeIndex(newIndex);

    CConfigSchemaHelper::getInstance()->setEnvTreeProp(pXPath, pRestriction->getEnumerationArray()->item(newIndex).getValue());
}

const char* getTableValue(const char *pXPath, int nRow)
{
    assert(pXPath != NULL && *pXPath != 0);

    CAttribute *pAttribute = NULL;
    CElement *pElement = NULL;

    if (CConfigSchemaHelper::isXPathTailAttribute(pXPath) == true)
       pAttribute = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getAttributeFromXPath(pXPath);

    if (pAttribute == NULL)
    {
        pElement =  CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getElementFromXPath(pXPath);
        assert(pElement != NULL);

        return pAttribute->getInstanceValue();
    }
    else
    {
        assert(pAttribute != NULL);
        if (nRow == 1)
            return pAttribute->getInstanceValue();
        else
        {
            StringBuffer strXPath(pXPath);
            const StringBuffer strXPathOriginal(pXPath);

            int offset = strXPathOriginal.length() - (CConfigSchemaHelper::stripXPathIndex(strXPath) + 1) ;
            CConfigSchemaHelper::stripXPathIndex(strXPath);

            strXPath.appendf("[%d]", nRow);
            strXPath.append(strXPathOriginal, offset, strXPathOriginal.length() - offset);
            pAttribute =  CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getAttributeFromXPath(strXPath.str());

            if (STRICTNESS_LEVEL >= DEFAULT_STRICTNESS)
                assert(pAttribute != NULL);

            if (pAttribute == NULL)
                return NULL;

            return pAttribute->getInstanceValue();
        }
    }
}

void setTableValue(const char *pXPath, int index, const char *pValue)
{
    UNIMPLEMENTED;
}

int getNumberOfUniqueColumns()
{
    return CConfigSchemaHelper::getInstance()->getEnvironmentXPathSize();
}

const char* getColumnName(int idx)
{
    if (idx < CConfigSchemaHelper::getInstance()->getEnvironmentXPathSize())
        return CConfigSchemaHelper::getInstance()->getEnvironmentXPaths(idx);
    else
        return NULL;
}

int getNumberOfRows(const char* pXPath)
{
    assert(pXPath != NULL && *pXPath != 0);
    PROGLOG("Get number of rows for %s = %d", pXPath, CConfigSchemaHelper::getInstance()->getElementArraySize(pXPath));

    return CConfigSchemaHelper::getInstance()->getElementArraySize(pXPath);
}

int getNumberOfTables()
{
    return CConfigSchemaHelper::getInstance()->getNumberOfTables();
}

const char* getServiceName(int idx, char *pName)
{
    if (pName != NULL)
        strcpy (pName, CBuildSetManager::getInstance()->getBuildSetServiceName(idx));

    return CBuildSetManager::getInstance()->getBuildSetServiceName(idx);
}

const char* getComponentName(int idx, char *pName)
{
    if (pName != NULL)
        strcpy (pName, CBuildSetManager::getInstance()->getBuildSetComponentName(idx));

    return CBuildSetManager::getInstance()->getBuildSetComponentName(idx);
}

int openConfigurationFile(const char* pFile)
{
    CConfigSchemaHelper::getInstance()->loadEnvFromConfig(pFile);
    return 1;
}

int getNumberOfComponentsInConfiguration(void *pData)
{
    if (pData == NULL)
    {
        return CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getNumberOfComponents();
    }
    else
    {
        CElement *pElement = static_cast<CElement*>(pData);
        assert(pElement->getNodeType() == XSD_ELEMENT);

        CElementArray *pElementArray = static_cast<CElementArray*>(pElement->getParentNode());
        assert(pElementArray->getNodeType() == XSD_ELEMENT_ARRAY);

        return pElementArray->length();
    }
}

void* getComponentInConfiguration(int idx)
{
    assert(idx < CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getNumberOfComponents());
    return CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx);
}

void* getComponentInstance(int idx, void *pData)
{
    assert(pData != NULL);
    assert(((static_cast<CElement*>(pData))->getNodeType()) == XSD_ELEMENT);

    CElement *pElement = static_cast<CElement*>(pData);
    CElementArray *pElementArray = static_cast<CElementArray*>(pElement->getParentNode());

    if (pElementArray->length() >= idx)
        idx = 0;

    return  &(pElementArray->item(idx));
}

const char* getComponentNameInConfiguration(int idx, void *pData)
{
    if (pData == NULL)
        return CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx)->getName();

    assert(!"Invalid component index");
    return NULL;
}


const void* getPointerToComponentInConfiguration(int idx, void *pData, int compIdx)
{
    if (pData == NULL)
    {
        const CElement *pElement = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx);
        assert(pElement != NULL);

        const CXSDNodeBase *pNodeBase = pElement->getConstParentNode();
        const CElementArray *pElementArray = dynamic_cast<const CElementArray*>(pNodeBase);
        assert(pElementArray != NULL);

        return pElementArray;
    }
    else
    {
        assert( compIdx >= 0);
        CElementArray *pElementArray = static_cast<CElementArray*>(pData);
        assert(pElementArray->getNodeType() == XSD_ELEMENT_ARRAY);

        const CXSDNodeBase *pNodeBase = &(pElementArray->item(compIdx+idx));
        return(dynamic_cast<const CElement*>(pNodeBase));
    }
}

const void* getPointerToComponentTypeInConfiguration(void *pData)
{
    assert (pData != NULL);
    CElement *pElement = static_cast<CElement*>(pData);
    assert (pElement->getNodeType() == XSD_ELEMENT);

    CElementArray *pElementArray = static_cast<CElementArray*>(pElement->getParentNode());

    return &(pElementArray->item(0));
}

int getIndexOfParent(void *pData)
{
    assert (pData != NULL);
    assert((static_cast<CElement*>(pData))->getNodeType() == XSD_ELEMENT);

    int nIndexOfParent = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getIndexOfElement(static_cast<CElement*>(pData));
    assert(nIndexOfParent >= 0);

    return nIndexOfParent;
}

const void* getPointerToComponents()
{
    assert(CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(0)->getConstParentNode()->getNodeType() == XSD_ELEMENT_ARRAY);
    return (CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(0)->getConstParentNode());
}

int getNumberOfChildren(void *pData)
{
    int nRetVal = 0;

    if (pData == NULL)
    {
        assert(!"Should not be null"); // why ever null?
        return 0;
    }

    if (pData == (void*)(CEnvironmentModel::getInstance()))
        nRetVal = (static_cast<CEnvironmentModel*>(pData))->getNumberOfRootNodes();
    else // must be of type CEnvironmentModelNode*
    {
        CEnvironmentModelNode *pNode = static_cast<CEnvironmentModelNode*>(pData);
        nRetVal = pNode->getNumberOfChildren();
    }
    return nRetVal;
}

const char* getData(void *pData)
{
    if (pData == NULL)
        return NULL;

    return CEnvironmentModel::getInstance()->getData(static_cast<CEnvironmentModelNode*>(pData));
}

const char* getName(void *pData)
{
    if (pData == NULL)
        return NULL;

    return CEnvironmentModel::getInstance()->getInstanceName(static_cast<CEnvironmentModelNode*>(pData));
}

const char* getFileName(void *pData)
{
    if (pData == NULL)
        return NULL;

    return CEnvironmentModel::getInstance()->getXSDFileName(static_cast<CEnvironmentModelNode*>(pData));
}

void* getParent(void *pData)
{
    if (pData == NULL)
        return NULL;

    if (pData == (void*)(CEnvironmentModel::getInstance()->getRoot()))
       return (void*)(CEnvironmentModel::getInstance());
    else
       return (void*)(CEnvironmentModel::getInstance()->getParent(static_cast<CEnvironmentModelNode*>(pData)));
    }

void* getChild(void *pData, int idx)
{
    if (pData == NULL || pData == CEnvironmentModel::getInstance())
    {
        if (idx == 0)
            return (void*)(CEnvironmentModel::getInstance()->getRoot(0));
        return NULL;
    }
    else
        return (void*)(CEnvironmentModel::getInstance()->getChild(static_cast<CEnvironmentModelNode*>(pData), idx));
}

int getIndexFromParent(void *pData)
{
    CEnvironmentModelNode *pNode = static_cast<CEnvironmentModelNode*>(pData);

    if (pNode->getParent() == NULL)
        return 0; // Must be 'Environment' node

    const CEnvironmentModelNode *pGrandParent = pNode->getParent();

    int nChildren = pGrandParent->getNumberOfChildren();

    for (int idx = 0; idx < nChildren; idx++)
    {
        if (pNode == pGrandParent->getChild(idx))
            return idx;
    }

    assert(!"Should not reach here");
    return 0;
}

void* getRootNode(int idx)
{
    return (void*)(CEnvironmentModel::getInstance()->getRoot(idx));
}

void* getModel()
{
    return (void*)(CEnvironmentModel::getInstance());
}

void getQML(void *pData, char **pOutput, int nIdx)
{
    CConfigSchemaHelper::getInstance()->printQML(CONFIGURATOR_API::getFileName(pData), pOutput, nIdx);
}

const char* getQMLFromFile(const char *pXSD, int idx)
{
    if (pXSD == NULL || *pXSD == 0)
        return NULL;
    return NULL;
}

void getQMLByIndex(int idx, char *pOutput)
{
    const char *pFileName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);
    CConfigSchemaHelper::getInstance()->printQML(pFileName, &pOutput, 0);
}

const char* getDocBookByIndex(int idx)
{
    const char *pFileName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);
    return CConfigSchemaHelper::getInstance()->printDocumentation(pFileName);
}

bool saveConfigurationFile()
{
    return CConfigSchemaHelper::getInstance()->saveConfigurationFile();
}

int getNumberOfNotificationTypes()
{
    return CNotificationManager::getInstance()->getNumberOfNotificationTypes();
}

const char* getNotificationTypeName(int type)
{
    return CNotificationManager::getInstance()->getNotificationTypeName(type);
}

int getNumberOfNotifications(int type)
{
    enum ENotificationType eType = static_cast<ENotificationType>(type);
    return CNotificationManager::getInstance()->getNumberOfNotifications(eType);
}

const char* getNotification(int type, int idx)
{
    const char *pRet = NULL;
    enum ENotificationType eType = static_cast<ENotificationType>(type);

    return CNotificationManager::getInstance()->getNotification(eType, idx);
}
} // CONFIGURATOR_API namespace
