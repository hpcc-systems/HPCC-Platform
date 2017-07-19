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

#ifndef _SECLIB_HPP__
#define _SECLIB_HPP__

#include "jlib.hpp"
#include "jtime.hpp"
#include "jexcept.hpp"

#ifndef SECLIB_API
#ifndef SECLIB_EXPORTS
    #define SECLIB_API DECL_IMPORT
#else
    #define SECLIB_API DECL_EXPORT
#endif //SECLIB_EXPORTS
#endif 

#define SECLIB "seclib"
#define LDAPSECLIB "LdapSecurity"

enum NewSecAccessFlags : int
{
    NewSecAccess_None = 0,
    NewSecAccess_Access = 1,
    NewSecAccess_Read = 2,
    NewSecAccess_Write = 4,
    NewSecAccess_Full = 255
};



enum SecAccessFlags : int
{
    SecAccess_Unavailable = -1,
    SecAccess_Unknown = -255,
    SecAccess_None = 0,
    SecAccess_Access = 1,
    SecAccess_Read = 3,
    SecAccess_Write = 7,
    SecAccess_Full = 255
};

static const char * getSecAccessFlagName(SecAccessFlags flag)
{
	switch (flag)
	{
		case SecAccess_Unavailable:
			return "Unavailable";
		case SecAccess_None:
			return "None";
		case SecAccess_Access:
			return "Access";
		case SecAccess_Read:
			return "Read";
		case SecAccess_Write:
			return "Write";
		case SecAccess_Full:
			return "Full";
		case SecAccess_Unknown:
		default:
			return "Unknown";
			break;
	}
}


enum SecResourceType : int
{
    RT_DEFAULT = 0,
    RT_MODULE = 1,
    RT_SERVICE = 2,
    RT_FILE_SCOPE = 3,
    RT_WORKUNIT_SCOPE = 4,
    RT_SUDOERS = 5,
    RT_TRIAL = 6,
    RT_VIEW_SCOPE = 7,
    RT_SCOPE_MAX = 8
};



const char* resTypeDesc(SecResourceType type);

enum SecPermissionType : int
{
    PT_DEFAULT = 0,
    PT_ADMINISTRATORS_ONLY = 1,
    PT_ADMINISTRATORS_AND_USER = 2  //excludes Authenticated users
};



#define DEFAULT_REQUIRED_ACCESS SecAccess_Read

enum SecPasswordEncoding : int
{
    SecPwEnc_unknown = 0,
    SecPwEnc_plain_text = 1,
    SecPwEnc_salt_sha1 = 2,
    SecPwEnc_salt_md5 = 3,
    SecPwEnc_Rijndael = 4,
    SecPwEnc_salt_accurint_md5 = 5
};


 
enum SecUserStatus : int
{
    SecUserStatus_Inhouse = 0,
    SecUserStatus_Active = 1,
    SecUserStatus_Exempt = 2,
    SecUserStatus_FreeTrial = 3,
    SecUserStatus_csdemo = 4,
    SecUserStatus_Rollover = 5,
    SecUserStatus_Suspended = 6,
    SecUserStatus_Terminated = 7,
    SecUserStatus_TrialExpired = 8,
    SecUserStatus_Status_Hold = 9,
    SecUserStatus_Unknown = 10
};


const static int scPasswordExpired = -1;
const static int scPasswordNeverExpires = -2;

interface ISecCredentials : extends IInterface
{
    virtual bool setPassword(const char * pw) = 0;
    virtual const char * getPassword() = 0;
    virtual void setSessionToken(const MemoryBuffer * const token) = 0;
    virtual const MemoryBuffer & getSessionToken() = 0;
    virtual void setSignature(const MemoryBuffer * const signature) = 0;
    virtual const MemoryBuffer & getSignature() = 0;
    virtual bool setPasswordExpiration(CDateTime & expirationDate) = 0;
    virtual CDateTime & getPasswordExpiration(CDateTime & expirationDate) = 0;
    virtual int getPasswordDaysRemaining() = 0;
};

//LDAP authentication status
enum authStatus : int
{
    AS_AUTHENTICATED = 0,
    AS_UNKNOWN = 1,//have not attempted to authenticate
    AS_UNEXPECTED_ERROR = 2,
    AS_INVALID_CREDENTIALS = 3,
    AS_PASSWORD_EXPIRED = 4,
    AS_PASSWORD_VALID_BUT_EXPIRED = 5//user entered valid password, but authentication failed because it is expired
};

class CDateTime;
interface ISecUser : extends IInterface
{
    virtual const char * getName() = 0;
    virtual bool setName(const char * name) = 0;
    virtual const char * getFullName() = 0;
    virtual bool setFullName(const char * name) = 0;
    virtual const char * getFirstName() = 0;
    virtual bool setFirstName(const char * fname) = 0;
    virtual const char * getLastName() = 0;
    virtual bool setLastName(const char * lname) = 0;
    virtual const char * getEmployeeID() = 0;
    virtual bool setEmployeeID(const char * emplID) = 0;
    virtual const char * getDistinguishedName() = 0;
    virtual bool setDistinguishedName(const char * dn) = 0;
    virtual const char * getRealm() = 0;
    virtual bool setRealm(const char * realm) = 0;
    virtual const char * getFqdn() = 0;
    virtual bool setFqdn(const char * Fqdn) = 0;
    virtual const char * getPeer() = 0;
    virtual bool setPeer(const char * Peer) = 0;
    virtual SecUserStatus getStatus() = 0;
    virtual bool setStatus(SecUserStatus Status) = 0;
    virtual authStatus getAuthenticateStatus() = 0;
    virtual void setAuthenticateStatus(authStatus status) = 0;
    virtual ISecCredentials & credentials() = 0;
    virtual unsigned getUserID() = 0;
    virtual void copyTo(ISecUser & destination) = 0;
    virtual CDateTime & getPasswordExpiration(CDateTime & expirationDate) = 0;
    virtual bool setPasswordExpiration(CDateTime & expirationDate) = 0;
    virtual int getPasswordDaysRemaining() = 0;
    virtual void setProperty(const char * name, const char * value) = 0;
    virtual const char * getProperty(const char * name) = 0;
    virtual void setPropertyInt(const char * name, int value) = 0;
    virtual int getPropertyInt(const char * name) = 0;
    virtual ISecUser * clone() = 0;
};


interface ISecAuthenticEvents : extends IInterface
{
    virtual bool onAuthenticationSuccess(ISecUser & User) = 0;
    virtual bool onAuthenticationFailure(ISecUser & User, unsigned reason, const char * description) = 0;
    virtual bool onRealmRequired(ISecUser & User) = 0;
    virtual bool onPasswordRequired(ISecUser & User, void * salt, unsigned salt_len) = 0;
    virtual bool onTokenRequired(ISecUser & User, unsigned type, void * salt, unsigned salt_len) = 0;
};




interface ISecProperty : extends IInterface
{
    virtual const char * getName() = 0;
    virtual const char * getValue() = 0;
};





interface ISecResource : extends ISecProperty
{
    virtual void setAccessFlags(SecAccessFlags flags) = 0;
    virtual SecAccessFlags getAccessFlags() = 0;
    virtual void setRequiredAccessFlags(SecAccessFlags flags) = 0;
    virtual SecAccessFlags getRequiredAccessFlags() = 0;
    virtual int addParameter(const char * name, const char * value) = 0;
    virtual const char * getParameter(const char * name) = 0;
    virtual void setDescription(const char * description) = 0;
    virtual const char * getDescription() = 0;
    virtual ISecResource * clone() = 0;
    virtual void copy(ISecResource * from) = 0;
    virtual SecResourceType getResourceType() = 0;
    virtual void setResourceType(SecResourceType resourcetype) = 0;
    virtual StringBuffer & toString(StringBuffer & s) = 0;
};


interface ISecPropertyIterator : extends IIteratorOf<ISecProperty>
{
};

interface ISecPropertyList : extends IInterface
{
    virtual ISecPropertyIterator * getPropertyItr() = 0;
    virtual ISecProperty * findProperty(const char * name) = 0;
};


interface ISecResourceList : extends ISecPropertyList
{
    virtual bool isAuthorizationComplete() = 0;
    virtual ISecResourceList * clone() = 0;
    virtual bool copyTo(ISecResourceList & destination) = 0;
    virtual void clear() = 0;
    virtual ISecResource * addResource(const char * name) = 0;
    virtual void addResource(ISecResource * resource) = 0;
    virtual bool addCustomResource(const char * name, const char * config) = 0;
    virtual ISecResource * getResource(const char * feature) = 0;
    virtual ISecResource * queryResource(unsigned seq) = 0;
    virtual int count() = 0;
    virtual const char * getName() = 0;
    virtual StringBuffer & toString(StringBuffer & s) = 0;
};


typedef IArrayOf<ISecUser> IUserArray;
typedef IArrayOf<ISecResource> IResourceArray;
typedef IArrayOf<ISecProperty> IPropertyArray;

interface ISecUserIterator : extends IIteratorOf<ISecUser>
{
};

interface IAuthMap : extends IInterface
{
    virtual int add(const char * path, ISecResourceList * resourceList) = 0;
    virtual bool shouldAuth(const char * path) = 0;
    virtual ISecResourceList * queryResourceList(const char * path) = 0;
    virtual ISecResourceList * getResourceList(const char * path) = 0;
};

enum secManagerType : int
{
    SMT_New,
    SMT_Default,
    SMT_Local,
    SMT_LDAP,
    SMT_HTPasswd,
    SMT_HTPluggable
};
interface IEspSecureContext;
interface ISecManager : extends IInterface
{
    virtual ISecUser * createUser(const char * user_name) = 0;
    virtual ISecResourceList * createResourceList(const char * rlname) = 0;
    virtual bool subscribe(ISecAuthenticEvents & events) = 0;
    virtual bool unsubscribe(ISecAuthenticEvents & events) = 0;
    virtual bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext) = 0;
    virtual bool authorizeEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = NULL) = 0;
    virtual SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext = NULL) = 0;
    virtual SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename) = 0;
    virtual SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope) = 0;
    virtual bool authorizeFileScope(ISecUser & user, ISecResourceList * resources) = 0;
    virtual bool addResources(ISecUser & user, ISecResourceList * resources) = 0;
    virtual bool addResourcesEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, SecPermissionType ptype, const char * basedn) = 0;
    virtual bool addResourceEx(SecResourceType rtype, ISecUser & user, const char * resourcename, SecPermissionType ptype, const char * basedn) = 0;
    virtual bool getResources(SecResourceType rtype, const char * basedn, IResourceArray & resources) = 0;
    virtual bool updateResources(ISecUser & user, ISecResourceList * resources) = 0;
    virtual bool updateSettings(ISecUser & user, ISecPropertyList * resources, IEspSecureContext* secureContext) = 0;
    virtual bool addUser(ISecUser & user) = 0;
    virtual ISecUser * findUser(const char * username) = 0;
    virtual ISecUser * lookupUser(unsigned uid) = 0;
    virtual ISecUserIterator * getAllUsers() = 0;
    virtual void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions ) = 0;
    virtual bool updateUserPassword(ISecUser & user, const char * newPassword, const char* currPassword = 0) = 0;
    virtual bool initUser(ISecUser & user) = 0;
    virtual void setExtraParam(const char * name, const char * value) = 0;
    virtual IAuthMap * createAuthMap(IPropertyTree * authconfig) = 0;
    virtual IAuthMap * createFeatureMap(IPropertyTree * authconfig) = 0;
    virtual IAuthMap * createSettingMap(IPropertyTree * authconfig) = 0;
    virtual void deleteResource(SecResourceType rtype, const char * name, const char * basedn) = 0;
    virtual void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn) = 0;
    virtual void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn) = 0;
    virtual void cacheSwitch(SecResourceType rtype, bool on) = 0;
    virtual bool authTypeRequired(SecResourceType rtype) = 0;
    virtual SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * filescope) = 0;
    virtual bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources) = 0;
    virtual const char * getDescription() = 0;
    virtual unsigned getPasswordExpirationWarningDays() = 0;
    virtual bool createUserScopes() = 0;
    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes) = 0;
    virtual SecAccessFlags queryDefaultPermission(ISecUser& user) = 0;
    virtual bool clearPermissionsCache(ISecUser & user) = 0;
    virtual bool authenticateUser(ISecUser & user, bool * superUser) = 0;
    virtual secManagerType querySecMgrType() = 0;
    virtual const char* querySecMgrTypeName() = 0;
};

interface IRestartHandler : extends IInterface
{
    virtual void Restart() = 0;
};

interface IRestartManager : extends IInterface
{
    virtual void setRestartHandler(IRestartHandler * pRestartHandler) = 0;
};



#endif
