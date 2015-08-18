#ifndef _CONFIGURATOR_API_HPP_
#define _CONFIGURATOR_API_HPP_

#define MAX_ARRAY_X 100
#define MAX_ARRAY_Y 128

const char modelNames[MAX_ARRAY_X][MAX_ARRAY_Y] =
                                        {   "tableDataModel0",
                                            "tableDataModel1",
                                            "tableDataModel2",
                                            "tableDataModel3",
                                            "tableDataModel4",
                                            "tableDataModel5",
                                            "tableDataModel6",
                                            "tableDataModel7",
                                            "tableDataModel8",
                                            "tableDataModel9",
                                            "tableDataModel10",
                                            "tableDataModel11",
                                            "tableDataModel12",
                                            "tableDataModel13",
                                            "tableDataModel14",
                                            "tableDataModel15",
                                            "tableDataModel16",
                                            "tableDataModel17",
                                            "tableDataModel18",
                                            "tableDataModel19",
                                            "tableDataModel20",
                                            "tableDataModel21",
                                            "tableDataModel22",
                                            "tableDataModel23",
                                            "tableDataModel24",
                                            "tableDataModel25",
                                            "tableDataModel26",
                                            "tableDataModel27",
                                            "tableDataModel28",
                                            "tableDataModel29",
                                            "tableDataModel30",
                                            "tableDataModel31",
                                            "tableDataModel32",
                                            "tableDataModel33",
                                            "tableDataModel34",
                                            "tableDataModel35",
                                            "tableDataModel36",
                                            "tableDataModel37",
                                            "tableDataModel38",
                                            "tableDataModel39",
                                            "tableDataModel40",
                                            "tableDataModel41",
                                            "tableDataModel42",
                                            "tableDataModel43",
                                            "tableDataModel44",
                                            "tableDataModel45",
                                            "tableDataModel46",
                                            "tableDataModel47",
                                            "tableDataModel48",
                                            "tableDataModel49",
                                            "tableDataModel50",
                                            "tableDataModel51",
                                            "tableDataModel52",
                                            "tableDataModel53",
                                            "tableDataModel54",
                                            "tableDataModel55",
                                            "tableDataModel56",
                                            "tableDataModel57",
                                            "tableDataModel58",
                                            "tableDataModel59",
                                            "tableDataModel60",
                                            "tableDataModel61",
                                            "tableDataModel62",
                                            "tableDataModel63",
                                            "tableDataModel64",
                                            "tableDataModel65",
                                            "tableDataModel66",
                                            "tableDataModel67",
                                            "tableDataModel68",
                                            "tableDataModel69",
                                            "tableDataModel72",
                                            "tableDataModel73",
                                            "tableDataModel74",
                                            "tableDataModel75",
                                            "tableDataModel76",
                                            "tableDataModel77",
                                            "tableDataModel78",
                                            "tableDataModel79",
                                            "tableDataModel80",
                                            "tableDataModel81",
                                            "tableDataModel82",
                                            "tableDataModel83",
                                            "tableDataModel84",
                                            "tableDataModel85",
                                            "tableDataModel86",
                                            "tableDataModel87",
                                            "tableDataModel88",
                                            "tableDataModel89",
                                            "tableDataModel90",
                                            "tableDataModel91",
                                            "tableDataModel92",
                                            "tableDataModel93",
                                            "tableDataModel94",
                                            "tableDataModel95",
                                            "tableDataModel96",
                                            "tableDataModel97",
                                            "tableDataModel98",
                                            "tableDataModel99"
                                            /** When adding here updated MAX_ARRAY_X  up top ! **/
                                        };


namespace CONFIGURATOR_API
{

#ifdef CONFIGURATOR_LIB
extern "C" int initialize();
#endif // CONFIGURATOR_LIB

extern "C" int getNumberOfAvailableComponents();
extern "C" int getNumberOfAvailableServices();
extern "C" const char* getServiceName(int idx, char *pName = 0);
extern "C" const char* getComponentName(int idx, char *pName = 0);

extern "C" int getValue(const char *pXPath, char *pValue);
extern "C" void setValue(const char *pXPath, const char *pValue);
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


extern "C" const char* getQML(void *pData, int nIdx);
extern "C" const char* getQMLFromFile(const char *pXSD, int idx);

extern "C" const char* getQMLByIndex(int idx);
extern "C" const char* getDocBookByIndex(int idx);
extern "C" const char* getDojoByIndex(int idx);

extern "C" bool saveConfigurationFile();

extern "C" int getNumberOfNotificationTypes();
extern "C" const char* getNotificationTypeName(int type);
extern "C" int getNumberOfNotifications(int type);
extern "C" const char* getNotification(int type, int idx);


/*extern "C" void closeConfigurationFile();


extern "C" const char* getComponentNameInConfiguration(int index, char *pName = 0);
extern "C" int getNumberOfServicesInConfiguration();
extern "C" const char* getServiceNameInConfiguration(int index, char *pName = 0);

extern "C" void setActiveView(int bIsService, int idx);*/

//extern "C" void* getComponentType(int idx);
//extern "C" void* getComponentTypes(void *pComponentTyp, int idx);
//extern "C" void* getComponent(void *pComponent, int idx);



}


#endif // _CONFIGURATOR_API_HPP_
