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

#ifndef _ESP_roxiecontrol_HPP__
#define _ESP_roxiecontrol_HPP__

#include "jlib.hpp"
#include "jsocket.hpp"
#include "jptree.hpp"
#include "roxiecontrol.hpp"

//bool sendRoxieControlLock(ISocket *sock, bool allOrNothing, unsigned wait)

IPropertyTree *sendRoxieControlQuery(ISocket *sock, const char *msg, unsigned wait);
IPropertyTree *sendRoxieControlAllNodes(ISocket *sock, const char *msg, bool allOrNothing, unsigned wait);
IPropertyTree *sendRoxieControlAllNodes(const SocketEndpoint &ep, const char *msg, bool allOrNothing, unsigned wait);

#endif
