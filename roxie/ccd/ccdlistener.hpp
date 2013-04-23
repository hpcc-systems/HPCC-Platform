/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#ifndef _CCDLISTENER_INCL
#define _CCDLISTENER_INCL

#include <jlib.hpp>

interface IRoxieListener : extends IInterface
{
    virtual void start() = 0;
    virtual bool stop(unsigned timeout) = 0;
    virtual void stopListening() = 0;
    virtual void disconnectQueue() = 0;
    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errMsg, int errCode) = 0;
    virtual unsigned queryPort() const = 0;
    virtual const SocketEndpoint &queryEndpoint() const = 0;
    virtual bool suspend(bool suspendIt) = 0;

    virtual void runOnce(const char *query) = 0;
};

extern IRoxieListener *createRoxieSocketListener(unsigned port, unsigned poolSize, unsigned listenQueue, bool suspended);
extern IRoxieListener *createRoxieWorkUnitListener(unsigned poolSize, bool suspended);
extern bool suspendRoxieListener(unsigned port, bool suspended);
extern IArrayOf<IRoxieListener> socketListeners;
extern void disconnectRoxieQueues();

#endif
