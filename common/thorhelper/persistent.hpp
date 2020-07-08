/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef __PERSISTENT_HPP__
#define __PERSISTENT_HPP__

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

#include "jlib.hpp"
#include "jsocket.hpp"

#define DEFAULT_MAX_PERSISTENT_IDLE_TIME 60
#define DEFAULT_MAX_PERSISTENT_REQUESTS  100

enum class PersistentLogLevel { PLogNone=0, PLogMin=1, PLogNormal=5, PLogMax=10 };
enum class PersistentProtocol { ProtoTCP=0, ProtoTLS=1 };
interface IPersistentHandler : implements IInterface
{
    // Add a new socket to the pool for reuse
    virtual void add(ISocket* sock, SocketEndpoint* ep = nullptr, PersistentProtocol proto = PersistentProtocol::ProtoTCP) = 0;
    // Remove a socket from the pool
    virtual void remove(ISocket* sock) = 0;
    // Put a socket back to the pool for further reuse, or remove its record from the pool when "keep" is false
    virtual void doneUsing(ISocket* sock, bool keep, unsigned usesOverOne = 0) = 0;
    // Get one available socket from the pool
    virtual ISocket* getAvailable(SocketEndpoint* ep = nullptr, bool* pShouldClose = nullptr, PersistentProtocol proto = PersistentProtocol::ProtoTCP) = 0;
    virtual void stop(bool wait) = 0;
    virtual bool inDoNotReuseList(SocketEndpoint* ep) = 0;
};

interface IPersistentSelectNotify
{
    virtual bool notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* handler, bool shouldClose) = 0;
};

inline size32_t httpVerOffset() { return 5; /*strlen("HTTP ")*/ }
THORHELPER_API bool isHttpPersistable(const char* httpVer, const char* conHeader);
THORHELPER_API IPersistentHandler* createPersistentHandler(IPersistentSelectNotify* notify, int maxIdleTime = DEFAULT_MAX_PERSISTENT_IDLE_TIME, int maxReqs = DEFAULT_MAX_PERSISTENT_REQUESTS, PersistentLogLevel loglevel=PersistentLogLevel::PLogMin, bool enableDoNotReuseList=false);

#endif //__PERSISTENT_HPP__
