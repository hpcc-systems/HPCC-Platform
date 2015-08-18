#ifndef _MAIN_WINDOW_INTERFACE_H_
#define _MAIN_WINDOW_INTERFACE_H_

extern "C" int StartMainWindowUI(int argc, char *argv[]);
extern "C" void SetComponentList(int nCount, const char *pComponentList[]);
extern "C" void AddComponentToList(const char *pComponent);
extern "C" void SetServiceList(int argc, char *pServiceList[]);
extern "C" int ShowMainWindowUI();

#endif // _MAIN_WINDOW_INTERFACE_H_
