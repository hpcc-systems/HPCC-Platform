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

#ifndef _MAIN_WINDOW_INTERFACE_H_
#define _MAIN_WINDOW_INTERFACE_H_

extern "C" int StartMainWindowUI(int argc, char *argv[]);
extern "C" void SetComponentList(int nCount, const char *pComponentList[]);
extern "C" void AddComponentToList(const char *pComponent);
extern "C" void SetServiceList(int argc, char *pServiceList[]);
extern "C" int ShowMainWindowUI();

#endif // _MAIN_WINDOW_INTERFACE_H_
