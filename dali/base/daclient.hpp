/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef DACLIENT_HPP
#define DACLIENT_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#include "jlib.hpp"
#include "mpcomm.hpp"
#include "dasds.hpp"

extern da_decl bool initClientProcess(IGroup *servergrp, DaliClientRole role, unsigned mpport=0, const char *clientVersion=NULL, const char *minServerVersion=NULL, unsigned timeout=MP_WAIT_FOREVER);
extern da_decl bool reinitClientProcess(IGroup *servergrp, DaliClientRole role, const char *clientVersion=NULL, const char *minServerVersion=NULL, unsigned timeout=MP_WAIT_FOREVER); 
extern da_decl void closedownClientProcess();
extern da_decl bool daliClientActive();

#define DALI_SERVER_PORT     7070 // default Dali server port

interface IDaliClientShutdown : extends IInterface
{
    virtual void clientShutdown() = 0;
};
extern da_decl void addShutdownHook(IDaliClientShutdown &shutdown);
extern da_decl void removeShutdownHook(IDaliClientShutdown &shutdown);

interface IDaliClient_Exception: extends IException
{
};

enum DaliClientError
{
    DCERR_ok,
    DCERR_server_closed,                    // raised if dali server closed
    DCERR_version_incompatibility
};


extern da_decl IDaliClient_Exception *createClientException(DaliClientError err, const char *msg=NULL);

extern da_decl void connectLogMsgManagerToDali();
extern da_decl void disconnectLogMsgManagerFromDali();
extern da_decl void connectLogMsgListenerToDali();
extern da_decl void disconnectLogMsgListenerFromDali();

// initiates client session and updates dali pointed to by environment, unless daliIp supplied
extern da_decl bool updateDaliEnv(IPropertyTree *env, bool updateDaliEnv=false, const char *daliIp=NULL);

// Find the environment section for a given dali instance
extern da_decl IPropertyTree *findDaliProcess(IPropertyTree *env, const SocketEndpoint &dali);


// the class below fills in the Status/Servers branch
// @name, @node, @mpport @started are automatically filled in
// queryConnection can be used to set other fields (remember to commit!)

class da_decl CSDSServerStatus
{
    IRemoteConnection * conn;
public:

    CSDSServerStatus(const char *servername);
    ~CSDSServerStatus() { stop(); }
    void stop();                    // only needed if stopping before destructor

    // setting extra properties
    IPropertyTree *queryProperties() { return conn->queryRoot(); }
    void commitProperties() { conn->commit(); }

    void reload(){conn->reload();}
};

#endif
