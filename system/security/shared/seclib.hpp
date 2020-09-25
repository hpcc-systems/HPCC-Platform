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

inline const char * getSecAccessFlagName(SecAccessFlags flag)
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

inline SecAccessFlags getSecAccessFlagValue(const char *s)
{
    if (isEmptyString(s))
        return SecAccess_None;
    switch (tolower(*s))
    {
    case 'u':
        if (strieq("unavailable", s))
            return SecAccess_Unavailable;
        break;
    case 'n':
        if (strieq("none", s))
            return SecAccess_None;
        break;
    case 'a':
        if (strieq("access", s))
            return SecAccess_Access;
        break;
    case 'r':
        if (strieq("read", s))
            return SecAccess_Read;
        break;
    case 'w':
        if (strieq("write", s))
            return SecAccess_Write;
        break;
    case 'f':
        if (strieq("full", s))
            return SecAccess_Full;
        break;
    default:
        break;
    }
    return SecAccess_Unknown;
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
    virtual void setSessionToken(unsigned token) = 0;
    virtual unsigned getSessionToken() = 0;
    virtual void setSignature(const char * signature) = 0;
    virtual const char * getSignature() = 0;
    virtual bool setPasswordExpiration(CDateTime & expirationDate) = 0;
    virtual CDateTime & getPasswordExpiration(CDateTime & expirationDate) = 0;
    virtual int getPasswordDaysRemaining() = 0;
};

// Bit-field value associate with a security entity method. Bits are reused by multiple entities.
// As a convenience to developers, named feature bits should be named such that the relationship
// can be inferred.
using SecFeatureBit = uint64_t;
// Any combination of SecFeatureBit values. Combinations of bits describing multiple entities can't
// be detected by code. Developers are responsible for properly defining combinations of interest.
using SecFeatureSet = SecFeatureBit;
// A selector for the entity feature set to be used in a query or comparison.
enum SecFeatureSupportLevel
{
    SFSL_Unsafe,      // request results in an exception
    SFSL_Safe,        // request is consumed without error
    SFSL_Implemented, // request is handled logically
};

interface ISecObject : extends IInterface
{
    // Returns a mask representing an arbitrary number of features with the request level of support.
    virtual SecFeatureSet queryFeatures(SecFeatureSupportLevel level) const = 0;
    inline SecFeatureSet queryUnsafeFeatures() const { return queryFeatures(SFSL_Unsafe); }
    inline SecFeatureSet querySafeFeatures() const { return queryFeatures(SFSL_Safe); }
    inline SecFeatureSet queryImplementedFeatures() const { return queryFeatures(SFSL_Implemented); }

    // Returns true if all features in the input mask are included in the indicated feature set.
    inline bool checkFeatures(SecFeatureSupportLevel level, SecFeatureSet featureMask) const { return ((queryFeatures(level) & featureMask) == featureMask); }
    inline bool checkUnsafeFeatures(SecFeatureSet featureMask) const { return checkFeatures(SFSL_Unsafe, featureMask); }
    inline bool checkSafeFeatures(SecFeatureSet featureMask) const { return checkFeatures(SFSL_Safe, featureMask); }
    inline bool checkImplementedFeatures(SecFeatureSet featureMask) const { return checkFeatures(SFSL_Implemented, featureMask); }
};

//LDAP authentication status
enum authStatus : int
{
    AS_AUTHENTICATED = 0,
    AS_UNKNOWN = 1,//have not attempted to authenticate
    AS_UNEXPECTED_ERROR = 2,
    AS_INVALID_CREDENTIALS = 3,
    AS_PASSWORD_EXPIRED = 4,
    AS_PASSWORD_VALID_BUT_EXPIRED = 5,//user entered valid password, but authentication failed because it is expired
    AS_ACCOUNT_DISABLED = 6,//valid username and password/credential are supplied but the account has been disabled
    AS_ACCOUNT_EXPIRED = 7,//valid username and password/credential supplied but the account has expired
    AS_ACCOUNT_LOCKED = 8,//valid username is supplied, but the account is locked out
};

static const SecFeatureBit SUF_NO_FEATURES                 = 0x00;
static const SecFeatureBit SUF_GetName                     = 0x01;
static const SecFeatureBit SUF_SetName                     = 0x02;
static const SecFeatureBit SUF_GetFullname                 = 0x04;
static const SecFeatureBit SUF_SetFullName                 = 0x08;
static const SecFeatureBit SUF_GetFirstName                = 0x10;
static const SecFeatureBit SUF_SetFirstName                = 0x20;
static const SecFeatureBit SUF_GetLastName                 = 0x40;
static const SecFeatureBit SUF_SetLastName                 = 0x80;
static const SecFeatureBit SUF_GetEmployeeID               = 0x0100;
static const SecFeatureBit SUF_SetEmployeeID               = 0x0200;
static const SecFeatureBit SUF_GetEmployeeNumber           = 0x0400;
static const SecFeatureBit SUF_SetEmployeeNumber           = 0x0800;
static const SecFeatureBit SUF_GetDistinguishedName        = 0x1000;
static const SecFeatureBit SUF_SetDistinguishedName        = 0x2000;
static const SecFeatureBit SUF_GetRealm                    = 0x4000;
static const SecFeatureBit SUF_SetRealm                    = 0x8000;
static const SecFeatureBit SUF_GetFqdn                     = 0x010000;
static const SecFeatureBit SUF_SetFqdn                     = 0x020000;
static const SecFeatureBit SUF_GetPeer                     = 0x040000;
static const SecFeatureBit SUF_SetPeer                     = 0x080000;
static const SecFeatureBit SUF_GetStatus                   = 0x100000;
static const SecFeatureBit SUF_SetStatus                   = 0x200000;
static const SecFeatureBit SUF_GetAuthenticateStatus       = 0x400000;
static const SecFeatureBit SUF_SetAuthenticateStatus       = 0x800000;
static const SecFeatureBit SUF_Credentials                 = 0x01000000;
static const SecFeatureBit SUF_GetUserID                   = 0x02000000;
static const SecFeatureBit SUF_CopyTo                      = 0x04000000;
static const SecFeatureBit SUF_GetPasswordExpiration       = 0x08000000;
static const SecFeatureBit SUF_SetPasswordExpiration       = 0x10000000;
static const SecFeatureBit SUF_GetPasswordDaysRemaining    = 0x20000000;
static const SecFeatureBit SUF_GetProperty                 = 0x40000000;
static const SecFeatureBit SUF_SetProperty                 = 0x80000000;
static const SecFeatureBit SUF_GetPropertyInt              = 0x0100000000;
static const SecFeatureBit SUF_SetPropertyInt              = 0x0200000000;
static const SecFeatureBit SUF_GetPropertyIterator         = 0x0400000000;
static const SecFeatureBit SUF_Clone                       = 0x0800000000;
static const SecFeatureBit SUF_GetDataElement              = 0x1000000000;
static const SecFeatureBit SUF_GetDataElements             = 0x2000000000;
static const SecFeatureBit SUF_SetData                     = 0x4000000000;
static const SecFeatureSet SUF_ALL_FEATURES                = 0x7FFFFFFFFF; // update to include all added feature bits

class CDateTime;
interface IPropertyIterator;
interface IPropertyTreeIterator;
interface ISecUser : implements ISecObject
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
    virtual const char * getEmployeeNumber() = 0;
    virtual bool setEmployeeNumber(const char * emplNumber) = 0;
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
    virtual IPropertyIterator * getPropertyIterator() const = 0;
    virtual ISecUser * clone() = 0;
    virtual IPropertyTree* getDataElement(const char* xpath = ".") const = 0;
    virtual IPropertyTreeIterator* getDataElements(const char* xpath = ".") const = 0;
    virtual bool setData(IPropertyTree* data) = 0;
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
    virtual void setValue(const char * value) = 0;
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
    virtual IPropertyIterator * getParameterIterator() const = 0;
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

interface ISecManager;
interface IEspSecureContext;
interface IAuthMap : extends IInterface
{
    virtual int add(const char * path, ISecResourceList * resourceList) = 0;
    virtual bool shouldAuth(const char * path) = 0;
    virtual ISecResourceList * queryResourceList(const char * path) = 0;
    virtual ISecResourceList * getResourceList(const char * path) = 0;
    virtual bool shareWithManager(ISecManager& manager, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool removeFromManager(ISecManager& manager, IEspSecureContext* secureContext = nullptr) = 0;
};

enum secManagerType : int
{
    SMT_New,
    SMT_Default,
    SMT_Local,
    SMT_LDAP,
    SMT_HTPasswd,
    SMT_SingleUser,
    SMT_HTPluggable,
    SMT_JWTAuth
};

static const SecFeatureBit SMF_NO_FEATURES                    = 0x00;
static const SecFeatureBit SMF_CreateUser                     = 0x01;
static const SecFeatureBit SMF_CreateResourceList             = 0x02;
static const SecFeatureBit SMF_SubscribeFullname              = 0x04;
static const SecFeatureBit SMF_Unsubscribe                    = 0x08;
static const SecFeatureBit SMF_Authorize                      = 0x10;
static const SecFeatureBit SMF_AuthorizeEx_List               = 0x20;
static const SecFeatureBit SMF_AuthorizeEx_Named              = 0x40;
static const SecFeatureBit SMF_GetAccessFlagsEx               = 0x80;
static const SecFeatureBit SMF_AuthorizeFileScope_List        = 0x0100;
static const SecFeatureBit SMF_AuthorizeFileScope_Named       = 0x0200;
static const SecFeatureBit SMF_AddResources                   = 0x0400;
static const SecFeatureBit SMF_AddResourcesEx_List            = 0x0800;
static const SecFeatureBit SMF_AddResourcesEx_Named           = 0x1000;
static const SecFeatureBit SMF_GetResources                   = 0x2000;
static const SecFeatureBit SMF_UpdateResources                = 0x4000;
static const SecFeatureBit SMF_UpdateSettings                 = 0x8000;
static const SecFeatureBit SMF_AddUser                        = 0x010000;
static const SecFeatureBit SMF_FindUser                       = 0x020000;
static const SecFeatureBit SMF_LookupUser                     = 0x040000;
static const SecFeatureBit SMF_GetAllUsers                    = 0x080000;
static const SecFeatureBit SMF_GetAllGroups                   = 0x100000;
static const SecFeatureBit SMF_UpdateUserPassword             = 0x200000;
static const SecFeatureBit SMF_InitUser                       = 0x400000;
static const SecFeatureBit SMF_SetExtraParam                  = 0x800000;
static const SecFeatureBit SMF_CreateAuthMap                  = 0x01000000;
static const SecFeatureBit SMF_CreateFeatureMap               = 0x02000000;
static const SecFeatureBit SMF_CreateSettingMap               = 0x04000000;
static const SecFeatureBit SMF_DeleteResource                 = 0x08000000;
static const SecFeatureBit SMF_RenameResource                 = 0x10000000;
static const SecFeatureBit SMF_CopyResource                   = 0x20000000;
static const SecFeatureBit SMF_CacheSwitch                    = 0x40000000;
static const SecFeatureBit SMF_AuthTypeRequired               = 0x80000000;
static const SecFeatureBit SMF_AuthorizeWorkUnitScope_List    = 0x0100000000;
static const SecFeatureBit SMF_AuthorizeWorkUnitScope_Named   = 0x0200000000;
static const SecFeatureBit SMF_GetDescription                 = 0x0400000000;
static const SecFeatureBit SMF_GetPasswordExpirationDays      = 0x0800000000;
static const SecFeatureBit SMF_CreateUserScopes               = 0x1000000000;
static const SecFeatureBit SMF_GetManagedScopeTree            = 0x2000000000;
static const SecFeatureBit SMF_QueryDefaultPermission         = 0x4000000000;
static const SecFeatureBit SMF_ClearPermissionsCache          = 0x8000000000;
static const SecFeatureBit SMF_AuthenticateUser               = 0x010000000000;
static const SecFeatureBit SMF_QuerySecMgrType                = 0x020000000000;
static const SecFeatureBit SMF_QuerySecMgrTypeName            = 0x040000000000;
static const SecFeatureBit SMF_LogoutUser                     = 0x080000000000;
static const SecFeatureBit SMF_RetrieveUserData               = 0x100000000000;
static const SecFeatureBit SMF_RemoveResources                = 0x200000000000;
static const SecFeatureSet SMF_ALL_FEATURES                   = 0x3FFFFFFFFFFF; // update to include all added feature bits

interface ISecManager : extends ISecObject
{
    virtual ISecUser * createUser(const char * user_name, IEspSecureContext* secureContext = nullptr) = 0;
    virtual ISecResourceList * createResourceList(const char * rlname, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool subscribe(ISecAuthenticEvents & events, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool unsubscribe(ISecAuthenticEvents & events, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool authorizeEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext = nullptr) = 0;
    virtual SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext = nullptr) = 0;
    virtual SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool authorizeFileScope(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool addResources(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool addResourcesEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, SecPermissionType ptype, const char * basedn, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool addResourceEx(SecResourceType rtype, ISecUser & user, const char * resourcename, SecPermissionType ptype, const char * basedn, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool getResources(SecResourceType rtype, const char * basedn, IResourceArray & resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool updateResources(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool updateSettings(ISecUser & user, ISecPropertyList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool addUser(ISecUser & user, IEspSecureContext* secureContext = nullptr) = 0;
    virtual ISecUser * findUser(const char * username, IEspSecureContext* secureContext = nullptr) = 0;
    virtual ISecUser * lookupUser(unsigned uid, IEspSecureContext* secureContext = nullptr) = 0;
    virtual ISecUserIterator * getAllUsers(IEspSecureContext* secureContext = nullptr) = 0;
    virtual void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool updateUserPassword(ISecUser & user, const char * newPassword, const char* currPassword = nullptr, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool initUser(ISecUser & user, IEspSecureContext* secureContext = nullptr) = 0;
    virtual void setExtraParam(const char * name, const char * value, IEspSecureContext* secureContext = nullptr) = 0;
    virtual IAuthMap * createAuthMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) = 0;
    virtual IAuthMap * createFeatureMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) = 0;
    virtual IAuthMap * createSettingMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) = 0;
    virtual void deleteResource(SecResourceType rtype, const char * name, const char * basedn, IEspSecureContext* secureContext = nullptr) = 0;
    virtual void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn, IEspSecureContext* secureContext = nullptr) = 0;
    virtual void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn, IEspSecureContext* secureContext = nullptr) = 0;
    virtual void cacheSwitch(SecResourceType rtype, bool on, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool authTypeRequired(SecResourceType rtype, IEspSecureContext* secureContext = nullptr) = 0;
    virtual SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * filescope, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
    virtual const char * getDescription() = 0;
    virtual unsigned getPasswordExpirationWarningDays(IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool createUserScopes(IEspSecureContext* secureContext = nullptr) = 0;
    virtual aindex_t getManagedScopeTree(SecResourceType rtype, const char * basedn, IArrayOf<ISecResource>& scopes, IEspSecureContext* secureContext = nullptr) = 0;
    virtual SecAccessFlags queryDefaultPermission(ISecUser& user, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool clearPermissionsCache(ISecUser & user, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool authenticateUser(ISecUser & user, bool * superUser, IEspSecureContext* secureContext = nullptr) = 0;
    virtual secManagerType querySecMgrType() = 0;
    virtual const char* querySecMgrTypeName() = 0;
    virtual bool logoutUser(ISecUser & user, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool retrieveUserData(ISecUser& requestedUser, ISecUser* requestingUser = nullptr, IEspSecureContext* secureContext = nullptr) = 0;
    virtual bool removeResources(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) = 0;
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
