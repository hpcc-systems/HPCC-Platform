/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#pragma once

#include "seclib.hpp"
#include "jscm.hpp"
#include "jthread.hpp"
#include "jtrace.hpp"

/**
 * @brief Templated utility class to decorate an object with additional functionality.
 *
 * A decorator implements the same named interface as the objects it decorates. This class extends
 * the interface, leaving implementation to subclasses. This class requires the decorated object to
 * implement a named interface, even if only IInterface.
 *
 * In the ideal situation, the decorated object's interface is described completely by a named
 * interface. By implementing the same interface, a decorator is interchangeable with the object
 * it decoratos.
 *
 * In less than ideal situations, the decorated object's interface is an extension of a named
 * interface. The decorator extends the extends the named interface, with subclasses required to
 * implement both the named interface and all extensions. The decorator should then be
 * interchangeable with its decorated object as a templated argument, but not be cast to the
 * decorated type.
 *
 * The less ideal scenario is suported by two template parameters. The ideal situation requires
 * only the first.
 * - decorated_t is the type of the object to be decorated. If the the decorated object conforms
 *   to an interface, use of the interface is preferred.
 * - secorated_interface_t is the interface implemented by the decorated object. If not the same
 *   as decorated_t, it is assumed to be a base of that type.
 *
 * Consider the example of ISecManager, generally, and the special case of CLdapSecManager. For
 * most security managers, both template parameters may be ISecManager. CLdapSecManager is an
 * exception because it exposes additional interfaces not included in ISecManager or any other
 * interface. In this case, decorated_t should be CLdapSecManager and decorated_interface_t should
 * be ISecManager.
 */
template <typename decorated_t, typename decorated_interface_t = decorated_t>
class TDecorator : public CInterfaceOf<decorated_interface_t>
{
protected:
    Linked<decorated_t> decorated;

public:
    TDecorator(decorated_t &_decorated) : decorated(&_decorated) {}
    virtual ~TDecorator() {}
    decorated_t *queryDecorated() { return decorated.get(); }
    decorated_t *getDecorated() { return decorated.getLink(); }
};

/**
 * @brief Macro used start tracing a block of code in the security manager decorator.
 *
 * Create a new named internal span and enter a try block. Used with END_SEC_MANAGER_TRACE_BLOCK,
 * provides consistent timing and exception handling for the inned code block.
 *
 * Some security manager requests include outgoing remote calls, but the ESP does not assume
 * which requests are remote. Managers making remote calls may considier creating client spans
 * as needed.
 */
#define START_SEC_MANAGER_TRACE_BLOCK(name)                                        \
    OwnedSpanScope spanScope(queryThreadedActiveSpan()->createInternalSpan(name)); \
    try                                                                            \
    {

/**
 * @brief Macro used to end tracing a block of code in the security manager decorator.
 *
 * Ends a try block and defines standard exception handling. Use with START_SEC_MANAGER_TRACE_BLOCK,
 * provides consistent timing and exception handling for the timed code block.
 */
#define END_SEC_MANAGER_TRACE_BLOCK                                        \
    }                                                                      \
    catch (IException * e)                                                 \
    {                                                                      \
        spanScope->recordException(e);                                     \
        throw;                                                             \
    }                                                                      \
    catch (...)                                                            \
    {                                                                      \
        Owned<IException> e(makeStringException(-1, "unknown exception")); \
        spanScope->recordException(e);                                     \
        throw;                                                             \
    }

/**
 * @brief Decorator for ISecManager that adds tracing to the interface.
 *
 * Tracing is added to selected methods of ISecManager by calling the decorated manager's method
 * within the scope of a new internal span. Tracing is conditional to reduce overhead:
 * - The `traceSecMgr` trace option must be enabled. Processes using security manager's that don't
 *   warrant tracing should disable the option.
 * - The decorated manager must claim method implementation through feature flags.  There is no
 *   mechanism by which the decorator can determine whether a decorated manager's implemented
 *   method warrants tracing.
 *
 * All methods not traced are passed through to the decorated manager instance.
 *
 * The default decorated type is sufficient for most security managers. CLdapSecManager is an
 * exception because platform processes depend on interfaces not included in the default.
 * - If ISecManager and CLdapSecManager interfaces are standardized, this point becomes moot.
 * - If ISecManager is extended to declare LDAP-specific interfaces, and CLdapSecManager implements
 *   this interface, create a subclass of TSecManagerTraceDecorator to decorate the new interface
 *   and replace ISecManager with the new interface name as template parameters.
 * - If nothing else changes, create a subclasses of TSecManagerTraceDecorator, with template
 *   parameters CLdapSecManager and ISecManager, to decorate the LDAP-specific interfaces.
 */
template <typename secmgr_t = ISecManager, typename secmgr_interface_t = ISecManager>
class TSecManagerTraceDecorator : public TDecorator<secmgr_t, secmgr_interface_t>
{
    using TDecorator<secmgr_t, secmgr_interface_t>::decorated;

public:
    virtual SecFeatureSet queryFeatures(SecFeatureSupportLevel level) const override
    {
        return decorated->queryFeatures(level);
    }
    virtual ISecUser *createUser(const char *user_name, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->createUser(user_name, secureContext);
    }
    virtual ISecResourceList *createResourceList(const char *rlname, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->createResourceList(rlname, secureContext);
    }
    virtual bool subscribe(ISecAuthenticEvents &events, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->subscribe(events, secureContext);
    }
    virtual bool unsubscribe(ISecAuthenticEvents &events, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->unsubscribe(events, secureContext);
    }
    virtual bool authorize(ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_Authorize))
            return decorated->authorize(user, resources, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize");
        ResourceListTracer tracer(user, RT_DEFAULT, resources);
        return tracer.authenticated = decorated->authorize(user, resources, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual bool authorizeEx(SecResourceType rtype, ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthorizeEx_List))
            return decorated->authorizeEx(rtype, user, resources, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize_ex.list");
        ResourceListTracer tracer(user, rtype, resources);
        return tracer.authenticated = decorated->authorizeEx(rtype, user, resources, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser &user, const char *resourcename, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthorizeEx_Named))
            return decorated->authorizeEx(rtype, user, resourcename, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize_ex.named");
        ResourceTracer tracer(user, rtype, resourcename);
        return tracer.accessFlags = decorated->authorizeEx(rtype, user, resourcename, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser &user, const char *resourcename, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->getAccessFlagsEx(rtype, user, resourcename, secureContext);
    }
    virtual SecAccessFlags authorizeFileScope(ISecUser &user, const char *filescope, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthorizeFileScope_Named))
            return decorated->authorizeFileScope(user, filescope, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize_file_scope.named");
        ResourceTracer tracer(user, RT_FILE_SCOPE, filescope);
        return tracer.accessFlags = decorated->authorizeFileScope(user, filescope, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual bool authorizeFileScope(ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthorizeFileScope_List))
            return decorated->authorizeFileScope(user, resources, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize_file_scope.list");
        ResourceListTracer tracer(user, RT_FILE_SCOPE, resources);
        return tracer.authenticated = decorated->authorizeFileScope(user, resources, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual bool addResources(ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->addResources(user, resources, secureContext);
    }
    virtual bool addResourcesEx(SecResourceType rtype, ISecUser &user, ISecResourceList *resources, SecPermissionType ptype, const char *basedn, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->addResourcesEx(rtype, user, resources, ptype, basedn, secureContext);
    }
    virtual bool addResourceEx(SecResourceType rtype, ISecUser &user, const char *resourcename, SecPermissionType ptype, const char *basedn, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->addResourceEx(rtype, user, resourcename, ptype, basedn, secureContext);
    }
    virtual bool getResources(SecResourceType rtype, const char *basedn, IResourceArray &resources, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->getResources(rtype, basedn, resources, secureContext);
    }
    virtual bool updateResources(ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->updateResources(user, resources, secureContext);
    }
    virtual bool updateSettings(ISecUser &user, ISecPropertyList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_UpdateSettings))
            return decorated->updateSettings(user, resources, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.update_settings");
        SettingListTracer tracer(user, resources);
        return tracer.authenticated = decorated->updateSettings(user, resources, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual bool addUser(ISecUser &user, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->addUser(user, secureContext);
    }
    virtual ISecUser *findUser(const char *username, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->findUser(username, secureContext);
    }
    virtual ISecUser *lookupUser(unsigned uid, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->lookupUser(uid, secureContext);
    }
    virtual ISecUserIterator *getAllUsers(IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->getAllUsers(secureContext);
    }
    virtual void getAllGroups(StringArray &groups, StringArray &managedBy, StringArray &descriptions, IEspSecureContext *secureContext = nullptr) override
    {
        decorated->getAllGroups(groups, managedBy, descriptions, secureContext);
    }
    virtual bool updateUserPassword(ISecUser &user, const char *newPassword, const char *currPassword = nullptr, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->updateUserPassword(user, newPassword, currPassword, secureContext);
    }
    virtual bool initUser(ISecUser &user, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->initUser(user, secureContext);
    }
    virtual void setExtraParam(const char *name, const char *value, IEspSecureContext *secureContext = nullptr) override
    {
        decorated->setExtraParam(name, value, secureContext);
    }
    virtual IAuthMap *createAuthMap(IPropertyTree *authconfig, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->createAuthMap(authconfig, secureContext);
    }
    virtual IAuthMap *createFeatureMap(IPropertyTree *authconfig, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->createFeatureMap(authconfig, secureContext);
    }
    virtual IAuthMap *createSettingMap(IPropertyTree *authconfig, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->createSettingMap(authconfig, secureContext);
    }
    virtual void deleteResource(SecResourceType rtype, const char *name, const char *basedn, IEspSecureContext *secureContext = nullptr) override
    {
        decorated->deleteResource(rtype, name, basedn, secureContext);
    }
    virtual void renameResource(SecResourceType rtype, const char *oldname, const char *newname, const char *basedn, IEspSecureContext *secureContext = nullptr) override
    {
        decorated->renameResource(rtype, oldname, newname, basedn, secureContext);
    }
    virtual void copyResource(SecResourceType rtype, const char *oldname, const char *newname, const char *basedn, IEspSecureContext *secureContext = nullptr) override
    {
        decorated->copyResource(rtype, oldname, newname, basedn, secureContext);
    }
    virtual void cacheSwitch(SecResourceType rtype, bool on, IEspSecureContext *secureContext = nullptr) override
    {
        decorated->cacheSwitch(rtype, on, secureContext);
    }
    virtual bool authTypeRequired(SecResourceType rtype, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->authTypeRequired(rtype, secureContext);
    }
    virtual SecAccessFlags authorizeWorkunitScope(ISecUser &user, const char *wuscope, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthorizeWorkUnitScope_Named))
            return decorated->authorizeWorkunitScope(user, wuscope, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize_work_unit_scope.named");
        ResourceTracer tracer(user, RT_WORKUNIT_SCOPE, wuscope);
        return tracer.accessFlags = decorated->authorizeWorkunitScope(user, wuscope, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual bool authorizeWorkunitScope(ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthorizeWorkUnitScope_List))
            return decorated->authorizeWorkunitScope(user, resources, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authorize_work_unit_scope.list");
        ResourceListTracer tracer(user, RT_WORKUNIT_SCOPE, resources);
        return tracer.authenticated = decorated->authorizeWorkunitScope(user, resources, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual const char *getDescription() override
    {
        return decorated->getDescription();
    }
    virtual unsigned getPasswordExpirationWarningDays(IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->getPasswordExpirationWarningDays(secureContext);
    }
    virtual aindex_t getManagedScopeTree(SecResourceType rtype, const char *basedn, IArrayOf<ISecResource> &scopes, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->getManagedScopeTree(rtype, basedn, scopes, secureContext);
    }
    virtual SecAccessFlags queryDefaultPermission(ISecUser &user, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->queryDefaultPermission(user, secureContext);
    }
    virtual bool clearPermissionsCache(ISecUser &user, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->clearPermissionsCache(user, secureContext);
    }
    virtual bool authenticateUser(ISecUser &user, bool *superUser, IEspSecureContext *secureContext = nullptr) override
    {
        if (!doTraceFeature(SMF_AuthenticateUser))
            return decorated->authenticateUser(user, superUser, secureContext);
        START_SEC_MANAGER_TRACE_BLOCK("security.authenticate_user");
        SuperUserTracer tracer(user, superUser);
        return tracer.authenticated = decorated->authenticateUser(user, superUser, secureContext);
        END_SEC_MANAGER_TRACE_BLOCK
    }
    virtual secManagerType querySecMgrType() override
    {
        return decorated->querySecMgrType();
    }
    virtual const char *querySecMgrTypeName() override
    {
        return decorated->querySecMgrTypeName();
    }
    virtual bool logoutUser(ISecUser &user, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->logoutUser(user, secureContext);
    }
    virtual bool retrieveUserData(ISecUser &requestedUser, ISecUser *requestingUser = nullptr, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->retrieveUserData(requestedUser, requestingUser, secureContext);
    }
    virtual bool removeResources(ISecUser &user, ISecResourceList *resources, IEspSecureContext *secureContext = nullptr) override
    {
        return decorated->removeResources(user, resources, secureContext);
    }

protected:
    SecFeatureSet implemented = 0; /// The decorated manager instance's implemented feature set.
    bool tracing = true; /// Flag indicating if tracing is enabled.

public:
    TSecManagerTraceDecorator(secmgr_t &_decorated)
        : TDecorator<secmgr_t, secmgr_interface_t>(_decorated)
        , implemented(_decorated.queryFeatures(SecFeatureSupportLevel::SFSL_Implemented))
        , tracing(queryTraceManager().isTracingEnabled() && doTrace(traceSecMgr))
    {
    }
    virtual ~TSecManagerTraceDecorator()
    {
    }

protected:
    /**
     * @brief Produce trace output describing a secure user.
     *
     * - Name and authenicated status are recorded unconditionally.
     * - User status is recorded when known.
     * - User properties are record when the trace level is at least traceDetailed.
     */
    struct UserTracer
    {
        ISecUser& user;
        bool authenticated = false;

        UserTracer(ISecUser& _user)
            : user(_user)
        {
            queryThreadedActiveSpan()->setSpanAttribute("user.name", user.getName());
        }

        ~UserTracer()
        {
            // Authorization and authentication requests with Boolean return values use the result
            // to indicate successful user authentication. Managers are not required to set the user
            // status to reflect this result. For brevity and clarity, express an affirmative
            // Boolean outcome as an authenticated status regardless of the user's actual status.
            authStatus as = user.getAuthenticateStatus();
            if (authenticated)
            {
                if (as != AS_AUTHENTICATED)
                    as = AS_AUTHENTICATED;
                else
                    authenticated = false;
            }

            ISpan* span = queryThreadedActiveSpan();
            span->setSpanAttribute("user.auth_status", map(as));
            if (user.getStatus() != SecUserStatus_Unknown)
                span->setSpanAttribute("user.status", map(user.getStatus()));
            if (doTrace(TraceFlags::Always, traceDetailed))
                traceProperties(span);
        }

        inline const char* map(authStatus updated) const
        {
            switch (updated)
            {
            case AS_AUTHENTICATED: return "authenticated";
            case AS_UNKNOWN: return "unknown";
            case AS_UNEXPECTED_ERROR: return "unexpected_error";
            case AS_INVALID_CREDENTIALS: return "invalid_credentials";
            case AS_PASSWORD_EXPIRED: return "password_expired";
            case AS_PASSWORD_VALID_BUT_EXPIRED: return "password_valid_but_expired";
            case AS_ACCOUNT_DISABLED: return "account_disabled";
            case AS_ACCOUNT_EXPIRED: return "account_expired";
            case AS_ACCOUNT_LOCKED: return "account_locked";
            case AS_ACCOUNT_ROOT_ACCESS_DENIED: return "account_root_access_denied";
            default: return "unexpected_status";
            }
        }

        inline const char* map(SecUserStatus updated) const
        {
            switch (updated)
            {
            case SecUserStatus_Inhouse: return "inhouse";
            case SecUserStatus_Active: return "active";
            case SecUserStatus_Exempt: return "exempt";
            case SecUserStatus_FreeTrial: return "free_trial";
            case SecUserStatus_csdemo: return "csdemo";
            case SecUserStatus_Rollover: return "rollover";
            case SecUserStatus_Suspended: return "suspended";
            case SecUserStatus_Terminated: return "terminated";
            case SecUserStatus_TrialExpired: return "trial_expired";
            case SecUserStatus_Status_Hold: return "status_hold";
            case SecUserStatus_Unknown: return "unknown";
            default: return "unexpected_status";
            }
        }

        void traceProperties(ISpan* span)
        {
            StringBuffer key("user.property.");
            size_t baseKeyLength = key.length();
            Owned<IPropertyIterator> props(user.getPropertyIterator());
            ForEach(*props)
            {
                const char* name = props->getPropKey();
                if (isEmptyString(name))
                    continue;
                const char* value = props->queryPropValue();
                if (isEmptyString(value))
                    value = "";
                key.setLength(baseKeyLength);
                getSnakeCase(key, name);
                span->setSpanAttribute(key, value);
            }
        }
    };

    /**
     * @brief Extends secure user tracing with super user status.
     *
     * `authenticateUser` introduces the concept of a *super user*. Super user status will be
     * recorded when requested by the caller.
     */
    struct SuperUserTracer : public UserTracer
    {
        bool *superUser;

        SuperUserTracer(ISecUser& _user, bool* _superUser)
            : UserTracer(_user)
            , superUser(_superUser)
        {
        }

        ~SuperUserTracer()
        {
            if (superUser)
                queryThreadedActiveSpan()->setSpanAttribute("user.super_user", (*superUser ? "true" : "false"));
        }
    };

    /**
     * @brief Extends secure user tracing with utility methods for security resource tracing.
     *
     * Resources authorized by name are handled differently from resources from a list, yet share
     * tracing requirements. This is a base class to provide shared functionality.
     */
    struct BaseResourceTracer : public UserTracer
    {
        using UserTracer::UserTracer;

        StringBuffer& getKey(StringBuffer& key, SecResourceType type, const char* name)
        {
            key.set(map(type)).append('.');
            getSnakeCase(key, name);
            return key;
        }

        inline const char* map(SecResourceType type) const
        {
            switch(type)
            {
            case RT_DEFAULT: return "permission";
            case RT_MODULE: return "module";
            case RT_SERVICE: return "service";
            case RT_FILE_SCOPE: return "file_scope";
            case RT_WORKUNIT_SCOPE: return "Workunit_scope";
            case RT_TRIAL: return "trial";
            case RT_VIEW_SCOPE: return "view";
            default: return "unknown";
            }
        }

        inline const char* map(SecAccessFlags updated) const
        {
            switch (updated)
            {
            case SecAccess_Full: return "full";
            case SecAccess_Read: return "read";
            case SecAccess_Write: return "write";
            case SecAccess_Access: return "access";
            case SecAccess_None: return "none";
            case SecAccess_Unavailable: return "unavailable";
            case SecAccess_Unknown: return "unknown";
            default: return "bad_access";
            }
        }
    };

    /**
     * @brief Extends secure user tracing with the access rights for a single named resource.
     *
     * User authentication status is implied by the access flags returned for the named resource.
     * A value of `SecAccess_Unavailable` is used to indicated to failed authentication. All
     * other values imply successful authentication.
     */
    struct ResourceTracer : public BaseResourceTracer
    {
        StringBuffer key;
        SecAccessFlags accessFlags = SecAccess_Unknown;

        ResourceTracer(ISecUser& _user, SecResourceType type, const char* name)
            : BaseResourceTracer(_user)
        {
            BaseResourceTracer::getKey(key, type, name);
        }

        ~ResourceTracer()
        {
            if (accessFlags != SecAccess_Unavailable)
                UserTracer::authenticated = true;
            queryThreadedActiveSpan()->setSpanAttribute(key, BaseResourceTracer::map(accessFlags));
        }
    };

    /**
     * @brief Extends secure user tracing with security resource access rights for any number of
     *        resources.
     *
     * All resources with access flags updated by the decorated manager will be recorded if tracing
     * is enabled. Unchanged resources will recorded when the trace level is elevated to (at least)
     * traceDetailed.
     */
    struct ResourceListTracer : public BaseResourceTracer
    {
        /**
         * @brief Short-term cache of a resource and associated state information used to control
         *        the output of the tracer that created it.
         */
        struct Resource
        {
            Linked<ISecResource> resource;
            SecResourceType type;
            SecAccessFlags originalAccess;

            Resource(ISecResource& _resource, SecResourceType _type)
                : resource(&_resource)
                , type(RT_DEFAULT == _type ? _resource.getResourceType() : _type)
                , originalAccess(_resource.getAccessFlags())
            {
            }
        };
        std::vector<Resource> resources;

        ResourceListTracer(ISecUser& user, SecResourceType type, ISecResourceList* _resources)
            : BaseResourceTracer(user)
        {
            if (_resources)
            {
                for (unsigned idx = 0, lmt = _resources->count(); idx < lmt; idx++)
                {
                    ISecResource* resource = _resources->queryResource(idx);
                    if (!resource)
                        continue;
                    resources.emplace_back(*resource, type);
                }
            }
        }

        ~ResourceListTracer()
        {
            ISpan* span = queryThreadedActiveSpan();
            StringBuffer key;
            bool recordUnchanged = doTrace(TraceFlags::Always, traceDetailed);
            for (const Resource& r : resources)
            {
                SecAccessFlags currentAccess = r.resource->getAccessFlags();
                if (r.originalAccess != currentAccess || currentAccess < SecAccess_None || recordUnchanged)
                {
                    BaseResourceTracer::getKey(key, r.type, r.resource->getName());
                    span->setSpanAttribute(key, BaseResourceTracer::map(currentAccess));
                }
            }
        }
    };

    /**
     * @brief Extends secure user tracing with secure user settings.
     *
     * User setting names and values are recorded when the trace level is at least detailed.
     */
    struct SettingListTracer : public UserTracer
    {
        ISecPropertyList* settings;

        SettingListTracer(ISecUser& user, ISecPropertyList* _settings)
            : UserTracer(user)
            , settings(_settings)
        {
        }

        ~SettingListTracer()
        {
            if (settings && doTrace(TraceFlags::Always, traceDetailed))
            {
                ISpan* span = queryThreadedActiveSpan();
                StringBuffer key("setting.");
                size_t baseKeyLen = key.length();
                Owned<ISecPropertyIterator> iter = settings->getPropertyItr();
                ForEach(*iter)
                {
                    const char* name = iter->query().getName();
                    if (isEmptyString(name))
                        continue;
                    const char* value = iter->query().getValue();
                    if (isEmptyString(value))
                        value = "";
                    key.setLength(baseKeyLen);
                    getSnakeCase(key, name);
                    span->setSpanAttribute(key, value);
                }
            }
        }
    };

    /**
     * @brief Determine if a method should be called directly or decorated with tracing.
     *
     * The determination is based on three factors, the trace manager state, the `traceSecMgr`
     * trace option, and the method's implented feature state. This method should not be invoked
     * for methods never presumed to warrant tracing as it makes no assumptions about the relevance
     * of tracing an individual method. Each overridden method should make this determination for
     * itself.
     *
     * @param feature The method feature flag to be tested.
     * @return true if security manager tracing is enabled and the feature is implemented
     * @return false if security manager tracing is disabled or the feature is not implemented
     */
    inline bool doTraceFeature(SecFeatureBit feature) const
    {
        return (tracing && (implemented & feature) != 0);
    }
};

/**
 * @brief Decorate for all ISecManager interfaces.
 *
 * Decoration of methods implemented by extensions to ISecManager requires a different class.
 */
using CSecManagerTraceDecorator = TSecManagerTraceDecorator<>;
