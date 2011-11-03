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

#ifndef DASESS_HPP
#define DASESS_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#ifdef _WIN32
#pragma warning(disable : 4786)
#endif

#include "dacoven.hpp"

typedef DALI_UID SessionId;
typedef DALI_UID SubscriptionId;
typedef DALI_UID SecurityToken;  // currently use 0

#define SESSID_DALI_SERVER ((SessionId)-1)   // used with subscribeSession


#define FIXED_HT_SIZE 4096 // elements
#define FIXED_KEY_SIZE 24

enum DaliClientRole // if changed must update queryRoleName()
{
    DCR_Unknown,
    DCR_Private,
    DCR_Diagnostic,
    DCR_ThorSlave,
    DCR_ThorMaster,
    DCR_HoleProcessor,
    DCR_HoleCollator,
    DCR_HoleServer,
    DCR_EclServer,
    DCR_EclAgent,
    DCR_DaliServer, // special (self)
    DCR_SashaServer,
    DCR_Util,
    DCR_Dfu,
    DCR_DfuServer,
    DCR_EspServer,
    DCR_WuClient, // GAB etc
    DCR_Config,
    DCR_Scheduler,
    DCR_RoxyMaster,
    DCR_RoxySlave,
    DCR_Other,
    DCR_BackupGen,
    DCR_AgentExec,
    DCR_EclScheduler,
    DCR_Max
};

interface IUserDescriptor: extends serializable
{
    virtual StringBuffer &getUserName(StringBuffer &buf)=0;
    virtual StringBuffer &getPassword(StringBuffer &buf)=0;
    virtual void set(const char *name,const char *password)=0;
    virtual void clear()=0;
};

extern da_decl IUserDescriptor *createUserDescriptor();


interface ISessionNotify: extends IInterface
{
    virtual void closed(SessionId id) = 0;
    virtual void aborted(SessionId id) = 0;
};

interface ISessionManager: extends IInterface
{
    virtual SessionId startSession(SecurityToken tok, SessionId parentid =-1) = 0;

    virtual void stopSession(SessionId, bool failed) = 0;   // session no longer valid after call

    virtual bool sessionStopped(SessionId, unsigned timeout) = 0;

    virtual SubscriptionId subscribeSession(SessionId id, ISessionNotify *inotify) = 0;
    virtual void unsubscribeSession(SubscriptionId id) = 0; // called from separate thread

    virtual SessionId lookupProcessSession(INode *node=NULL) = 0; // no parameters - get my session ID
    virtual INode *getProcessSessionNode(SessionId id) =0;        // if session not running returns null

    virtual StringBuffer &getClientProcessList(StringBuffer &buf)=0; // for diagnostics
    virtual StringBuffer &getClientProcessEndpoint(SessionId id,StringBuffer &buf)=0; // for diagnostics
    virtual unsigned queryClientCount() = 0; // for SNMP

    virtual int getPermissionsLDAP(const char *key,const char *obj,IUserDescriptor *udesc,unsigned auditflags, int *err=NULL)=0;
    virtual bool checkScopeScansLDAP()=0;
    virtual unsigned getLDAPflags()=0;
    virtual void setLDAPflags(unsigned flags)=0;

};

// the following are getPermissionsLDAP input flags for audit reporting
#define DALI_LDAP_AUDIT_REPORT              (1)         // required to check
#define DALI_LDAP_READ_WANTED               (2)
#define DALI_LDAP_WRITE_WANTED              (4)

#define HASREADPERMISSION(p)        (((p)&3)==3)        
#define HASWRITEPERMISSION(p)       (((p)&5)==5)
#define HASNOSECURITY(p)            ((p)==-1)           // security is disabled

extern da_decl ISessionManager &querySessionManager();

#define myProcessSession() (querySessionManager().lookupProcessSession())



// for server use
interface IDaliServer;
interface IPropertyTree;
interface IFile;
interface IDaliLdapConnection;
interface IDaliClientAuthConnection;
extern da_decl IDaliServer *createDaliSessionServer(); // called for coven members
extern da_decl void setLDAPconnection(IDaliLdapConnection *ldapconn); // called for coven members
extern da_decl void setClientAuth(IDaliClientAuthConnection *authconn); // called for coven members

#endif
