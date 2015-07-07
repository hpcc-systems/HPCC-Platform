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

    /*StringArray arrComps;

    CBuildSetManager::getInstance()->getBuildSetComponents(arrComps);

    return arrComps.length();*/
    return CBuildSetManager::getInstance()->getBuildSetComponentCount();
}

int getNumberOfAvailableServices()
{
    assert(s_pConfigSchemaHelper != NULL);

    /*StringArray arrServices;

    CBuildSetManager::getInstance()->getBuildSetServices(arrServices);

    return arrServices.length();*/
    return  CBuildSetManager::getInstance()->getBuildSetServiceCount();
}

/*const char* getBuildSetServiceName(int idx, char *pName)
{
    assert(s_pConfigSchemaHelper != NULL);

    if (pName != NULL)
    {
        strncpy(pName, CBuildSetManager::getInstance()->getBuildSetServiceName(idx), CBuildSetManager::getInstance()->getBuildSetServiceName(idx) == NULL ? 0 : strlen(CBuildSetManager::getInstance()->getBuildSetServiceName(idx)));
    }

    return CBuildSetManager::getInstance()->getBuildSetServiceName(idx);
}

const char* getBuildSetComponentName(int idx, char *pName)
{
    assert(s_pConfigSchemaHelper != NULL);

    if (pName != NULL)
    {
        strncpy(pName, CBuildSetManager::getInstance()->getBuildSetComponentName(idx), CBuildSetManager::getInstance()->getBuildSetComponentName(idx) == NULL ? 0 : strlen(CBuildSetManager::getInstance()->getBuildSetComponentName(idx)));
    }

    return CBuildSetManager::getInstance()->getBuildSetComponentName(idx);
}*/


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

    //assert(pAttribute != NULL);
    if(pAttribute == NULL)
    {
        std::cout << "xPath: " << pXPath << "| value: " << pXPath << std::endl;
    }
    else if (pAttribute->isInstanceValueValid() == true)
    {
        //strcpy(pValue, pAttribute->getEnvValueFromXML());
        strcpy(pValue, pAttribute->getInstanceValue());
        std::cout << "xPath: " << pXPath << "| value: " << pValue << std::endl;
    }
    /*else
    { // If attribute is valid but instance does not exist
        std::cout << "xPath: " << pXPath << "| value: NULL" << std::endl;
        pValue = NULL;
    }*/
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
    {
       pAttribute = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getAttributeFromXPath(pXPath);
    }

    if (pAttribute == NULL)
    {
        pElement =  CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getElementFromXPath(pXPath);

        assert(pElement != NULL);

        //return pElement->getEnvValueFromXML();
        return pAttribute->getInstanceValue();
    }
    else
    {
        assert(pAttribute != NULL);

        if (nRow == 1)
        {
            //return pAttribute->getEnvValueFromXML();
            return pAttribute->getInstanceValue();
        }
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
            {
                assert(pAttribute != NULL);
            }

            if (pAttribute == NULL)
            {
                return NULL;
            }


            //return pAttribute->getEnvValueFromXML();
            return pAttribute->getInstanceValue();
        }
    }
}

void setTableValue(const char *pXPath, int index, const char *pValue)
{
    assert(false);  // NOT IMPLEMENTED
}


int getNumberOfUniqueColumns()
{
    return CConfigSchemaHelper::getInstance()->getEnvironmentXPathSize();
}

const char* getColumnName(int idx)
{
    if (idx < CConfigSchemaHelper::getInstance()->getEnvironmentXPathSize())
    {
        return CConfigSchemaHelper::getInstance()->getEnvironmentXPaths(idx);
    }
    else
    {
        return NULL;
    }
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
    {
        strcpy (pName, CBuildSetManager::getInstance()->getBuildSetServiceName(idx));
    }
    return CBuildSetManager::getInstance()->getBuildSetServiceName(idx);
}

const char* getComponentName(int idx, char *pName)
{
    if (pName != NULL)
    {
        strcpy (pName, CBuildSetManager::getInstance()->getBuildSetComponentName(idx));
    }
    return CBuildSetManager::getInstance()->getBuildSetComponentName(idx);
}

int openConfigurationFile(const char* pFile)
{
    CConfigSchemaHelper::getInstance()->loadEnvFromConfig(pFile);

    return 1;
}

/*const char* getComponentNameInConfiguration(int idx, char *pName)
{
    if (idx < 0)
    {
        return NULL;
    }

    if (pName != NULL)
    {
        strcpy (pName, CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx)->getName());
    }

    return CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx)->getName();
}*/

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

        //int index = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getIndexOfElement(pElement);

        //CElementArray *pElementArray = (static_cast<CElementArray*>(CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(index)->getParentNode()));

        CElementArray *pElementArray = static_cast<CElementArray*>(pElement->getParentNode());

        assert(pElementArray->getNodeType() == XSD_ELEMENT_ARRAY);

        int nLength = pElementArray->length();

        return nLength;
    }
}

void* getComponentInConfiguration(int idx)
{
    assert(idx < CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getNumberOfComponents());

    CElement *pElement = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx);

    return pElement;
}

void* getComponentInstance(int idx, void *pData)
{
    assert(pData != NULL);
    assert(((static_cast<CElement*>(pData))->getNodeType()) == XSD_ELEMENT);

    CElement *pElement = static_cast<CElement*>(pData);

    CElementArray *pElementArray = static_cast<CElementArray*>(pElement->getParentNode());

    //assert(pElementArray->length() > idx);

    if (pElementArray->length() >= idx)
        idx = 0;

    CElement *pElement2 = &(pElementArray->item(idx));

    return pElement2;
}

const char* getComponentNameInConfiguration(int idx, void *pData)
{
    if (pData == NULL)
    {
        return CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx)->getName();
    }
    else
    {
        CElement *pElement = static_cast<CElement*>(pData);

        assert(pElement->getNodeType() == XSD_ELEMENT);

        pElement = &(dynamic_cast<const CElementArray*>(pElement->getConstParentNode())->item(idx));

        const CAttribute *pAttribute = NULL;

        /*if (pElement->getAttributeArray() != NULL)
        {
            pAttribute = pElement->getAttributeArray()->findAttributeWithName("name");

            return pAttribute->getName();
        }
        else
        {*/
            return NULL;
        //}
    }

    assert(false);
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

        const CElement *pReturnElement = dynamic_cast<const CElement*>(pNodeBase);

        return pReturnElement;
    }
}

const void* getPointerToComponentTypeInConfiguration(void *pData)
{
    assert (pData != NULL);

    CElement *pElement = static_cast<CElement*>(pData);

    assert (pElement->getNodeType() == XSD_ELEMENT);

    CElementArray *pElementArray = static_cast<CElementArray*>(pElement->getParentNode());

    pElement = &(pElementArray->item(0));

    return pElement;
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
        assert(false); // why ever null?
        return 0;
    }

    if (pData == (void*)(CEnvironmentModel::getInstance()))
    {
        nRetVal = (static_cast<CEnvironmentModel*>(pData))->getNumberOfRootNodes();
    }
    else // must be of type CEnvironmentModelNode*
    {
        CEnvironmentModelNode *pNode = static_cast<CEnvironmentModelNode*>(pData);

        nRetVal = pNode->getNumberOfChildren();
    }

    return nRetVal;
}

const char* getData(void *pData)
{
    //assert(pData != NULL);
    if (pData == NULL)
    {
        return NULL;
    }

    const char *p = CEnvironmentModel::getInstance()->getData(static_cast<CEnvironmentModelNode*>(pData));

    return p;
}

const char* getName(void *pData)
{
    if (pData == NULL)
    {
        return NULL;
    }

    const char *p = CEnvironmentModel::getInstance()->getInstanceName(static_cast<CEnvironmentModelNode*>(pData));

    return p;
}

const char* getFileName(void *pData)
{
    if (pData == NULL)
    {
        return NULL;
    }

    const char *p = CEnvironmentModel::getInstance()->getXSDFileName(static_cast<CEnvironmentModelNode*>(pData));

    return p;
}

void* getParent(void *pData)
{
    if (pData == NULL)
    {
        return NULL;
    }

    if (pData == (void*)(CEnvironmentModel::getInstance()->getRoot()))
    {
        return (void*)(CEnvironmentModel::getInstance());
    }
    else
    {
        return (void*)(CEnvironmentModel::getInstance()->getParent(static_cast<CEnvironmentModelNode*>(pData)));
        //return pData;
    }
}

void* getChild(void *pData, int idx)
{
    if (pData == NULL || pData == CEnvironmentModel::getInstance())
    {
        if (idx == 0)
        {
            return (void*)(CEnvironmentModel::getInstance()->getRoot(0));
        }

        //assert(idx < 1);
        return NULL;
    }
    else
    {
        void *pRetPtr = (void*)(CEnvironmentModel::getInstance()->getChild(static_cast<CEnvironmentModelNode*>(pData), idx));

        return pRetPtr;
    }
}

int getIndexFromParent(void *pData)
{
 //   assert(pData != NULL);

    CEnvironmentModelNode *pNode = static_cast<CEnvironmentModelNode*>(pData);

    //assert (pNode->getParent() != NULL);
    if (pNode->getParent() == NULL)
    {
        return 0; // Must be 'Environment' node
    }

    const CEnvironmentModelNode *pGrandParent = pNode->getParent();

    int nChildren = pGrandParent->getNumberOfChildren();

    for (int idx = 0; idx < nChildren; idx++)
    {
        if (pNode == pGrandParent->getChild(idx))
        {
            return idx;
        }
    }

    assert(false);
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

const char* getQML(void *pData, int nIdx)
{
    return CConfigSchemaHelper::getInstance()->printQML(getFileName(pData), nIdx);
}

const char* getQMLFromFile(const char *pXSD, int idx)
{
    if (pXSD == NULL || *pXSD == 0)
    {
        return NULL;
    }

    //const char *pFileName = ConfigSchemaHelper::getInstance()->getFileName(pData)

    //return CConfigSchemaHelper::getInstance()->printQML(pFileName, 0);
            return NULL;
}

const char* getQMLByIndex(int idx)
{
    const char *pFileName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);
    return (CConfigSchemaHelper::getInstance()->printQML(pFileName, 0));
}

const char* getDocBookByIndex(int idx)
{
    const char *pFileName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);
    return CConfigSchemaHelper::getInstance()->printDocumentation(pFileName);
}

const char* getDojoByIndex(int idx)
{
    const char *pFileName = CBuildSetManager::getInstance()->getBuildSetComponentFileName(idx);
    return CConfigSchemaHelper::getInstance()->printDojoJS(pFileName);
}

bool saveConfigurationFile()
{
    bool bRetVal = false;

    bRetVal = CConfigSchemaHelper::getInstance()->saveConfigurationFile();

    return bRetVal;
}

int getNumberOfNotificationTypes()
{
    int nRetVal = 0;

    nRetVal = CNotificationManager::getInstance()->getNumberOfNotificationTypes();

    return nRetVal;
}

const char* getNotificationTypeName(int type)
{
    const char *pRet = NULL;

    pRet = CNotificationManager::getInstance()->getNotificationTypeName(type);

    return pRet;
}

int getNumberOfNotifications(int type)
{
    int nRetVal = 0;
    enum ENotificationType eType = static_cast<ENotificationType>(type);

    nRetVal = CNotificationManager::getInstance()->getNumberOfNotifications(eType);

    return nRetVal;
}

const char* getNotification(int type, int idx)
{
    const char *pRet = NULL;
    enum ENotificationType eType = static_cast<ENotificationType>(type);

    pRet = CNotificationManager::getInstance()->getNotification(eType, idx);

    return pRet;
}

/*void* getComponent(void *pComponent, int idx)
{
    assert(idx >= 0);

    if (pComponenet == NULL)
    {
        return CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getComponent(idx)->getParentNode();
    }
    else
    {
        CElementArray *pElementArray = static_cast<CElementArray*>(pComponent);

        if (pElementArray->length() >= idx || idx < 0)
        {
            return NULL;
        }

        pElementArray->item()
    }
}*/


/*
void closeConfigurationFile()
{

}


const char* getComponentNameInConfiguration(int index, char *pName = 0)
{

}

int getNumberOfServicesInConfiguration()
{

}

const char* getServiceNameInConfiguration(int index, char *pName = 0)
{

}
*/

}
//#endif // CONFIGURATOR_LIB
