/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef DACLIENT_HPP
#define DACLIENT_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#include "mpcomm.hpp"
#include "dasds.hpp"

extern da_decl bool initClientProcess(IGroup *servergrp, DaliClientRole role, unsigned short mpport=0, const char *clientVersion=NULL, const char *minServerVersion=NULL, unsigned timeout=MP_WAIT_FOREVER,bool enableSEH=true); 
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

// initates client session and updates dali pointed to by environment, unless daliIp supplied
extern da_decl bool updateDaliEnv(IPropertyTree *env, bool updateDaliEnv=false, const char *daliIp=NULL);



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
