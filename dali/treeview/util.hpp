/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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



#ifndef _util_hpp
#define _util_hpp

#include "jsocket.hpp"


void showFIOErr(LPCSTR fname, bool open);

void putProfile(LPCSTR section, LPCSTR key, LPCSTR value);
void putProfile(LPCSTR section, LPCSTR key, int value);
LPCSTR getProfileStr(LPCSTR section, LPCSTR key);
int getProfileInt(LPCSTR section, LPCSTR key);

void toEp(SocketEndpoint &ep, LPCSTR eptxt);

void reportException(IException * e);


#endif