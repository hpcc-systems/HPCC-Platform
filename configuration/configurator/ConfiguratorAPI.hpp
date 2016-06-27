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

#ifndef _CONFIGURATOR_API_HPP_
#define _CONFIGURATOR_API_HPP_

#define MAX_ARRAY_X 100
#define MAX_ARRAY_Y 128

const char* getTableDataModelName(int index);
void deleteTableModels();


namespace CONFIGURATOR_API
{
//#ifdef CONFIGURATOR_LIB
    extern "C" int initialize();
//#endif // CONFIGURATOR_LIB

extern "C" int getNumberOfAvailableComponents();
extern "C" int getNumberOfAvailableServices();
extern "C" const char* getServiceName(int idx, char *pName = 0);
extern "C" const char* getComponentName(int idx, char *pName = 0);
extern "C" int getValue(const char *pXPath, char *pValue);
extern "C" bool setValue(const char *pXPath, const char *pValue);
extern "C" int getIndex(const char *pXPath);
extern "C" void setIndex(const char *pXPath, int newIndex);
extern "C" const char* getTableValue(const char *pXPath, int nRow);
extern "C" void setTableValue(const char *pXPath, int index, const char *pValue);
extern "C" int getNumberOfUniqueColumns();
extern "C" const char* getColumnName(int idx);
extern "C" int getNumberOfRows(const char* pXPath);
extern "C" int getNumberOfTables();
extern "C" int openConfigurationFile(const char* pFile);
extern "C" int getNumberOfComponentsInConfiguration(void *pData);
extern "C" void* getComponentInConfiguration(int idx);
extern "C" void* getComponentInstance(int idx, void *pData);
extern "C" const void* getPointerToComponentTypeInConfiguration(void *pData);
extern "C" const char* getComponentNameInConfiguration(int idx, void *pData);
extern "C" const void* getPointerToComponentInConfiguration(int idx, void *pData, int compIdx = -1);
extern "C" const void* getPointerToComponents();
extern "C" int getIndexOfParent(void *pData);
extern "C" int getNumberOfChildren(void *pData);
extern "C" const char* getData(void *pData);
extern "C" const char* getName(void *pData);
extern "C" const char* getFileName(void *pData);
extern "C" void* getParent(void *pData);
extern "C" void* getChild(void *pData, int idx);
extern "C" int getIndexFromParent(void *pData);
extern "C" void* getRootNode(int idx = 0);
extern "C" void* getModel();
extern "C" void reload(const char *pFile);
extern "C" void getJSON(void *pData, char **pOutput, int nIdx);
extern "C" void getNavigatorJSON(char **pOutput);
extern "C" void getJSONByComponentName(const char *pComponentName, char **pOutput, int nIdx);
extern "C" void getJSONByComponentKey(const char *pKey, char **pOutput);
extern "C" const char* getDocBookByIndex(int idx);
extern "C" bool saveConfigurationFile();
extern "C" bool saveConfigurationFileAs(const char *pFilePath);
extern "C" int getNumberOfNotificationTypes();
extern "C" const char* getNotificationTypeName(int type);
extern "C" int getNumberOfNotifications(int type);
extern "C" const char* getNotification(int type, int idx);
}

#endif // _CONFIGURATOR_API_HPP_
