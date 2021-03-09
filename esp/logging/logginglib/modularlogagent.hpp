/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef _MODULARLOGAGENT_HPP_
#define _MODULARLOGAGENT_HPP_

/**
 * The modular log agent framework defines an implementation of IEspLogAgent that delegates agent
 * requests to helper classes. This delegation enables a single agent plugin to support multiple
 * logging requirements by configuring which helpers are to be used by any single agent created
 * by the plugin.
 *
 * The framework includes at least one implementation of each type of helper. Plugins are not
 * required to use these implementations, nor are they required to reference them with "standard"
 * configuration keys. Consider that a plugin implementing a user's business logic may want its
 * default behavior to use the business logic in place of a default assigned by the platform.
 * Each plugin is encouraged to provide documentation of its exposed functionality.
 *
 * The "modularlogagent" platform plugin includes README.md, which describes the configuration of
 * multiple elements used by the agent. The properties of each element may vary from plugin to
 * plugin, but the elements are defined by the framework.
 *
 * Refer to interface and implementation declarations for additional details regarding available
 * modules. Refer to each plugin to know which modules are registered and how they may be accessed.
 */

#include "loggingagentbase.hpp"
#include "esptraceloggingcomponent.hpp"
#include "jlog.hpp"
#include "jstring.hpp"
#include <map>

interface IEsdlScriptContext;
interface IXpathContext;

/**
 * The framework interfaces and classes are defined within a namespace.
 *
 * Plugins, especially those built as standoalone entities, are suscenptible to name collisions.
 * All framework names are encapsulated in a namespace to reduce the risk of collisions.
 */
namespace ModularLogAgent
{
    interface IAgent;
    class CModuleFactory;

    ///
    /// Root element name of an agent configuration. The value is imposed by the logging
    /// framework and applies to all IEspLogAgent implementations.
    ///
    constexpr static const char* moduleAgent            = "LogAgent";

    /**
     * Implementation of IEspLogAgent interface that integrates modular log agent service (as
     * identified by the LOGServiceType enumeration) implementations into general logging framework.
     * Log content filters and variant identification are handled directly. All service
     * functionality is delegated to user-configured module implementations.
     *
     * Log content filters are always applied, but are not required. The application of filters,
     * even when none are configured, produces a side effect of combining update request fragments
     * into single XML document. The agent requires the combined document and assumes, given an
     * interface which accepts fragments, that the data given might not be in the required form.
     *
     * Agent variant identifiers are defined as they are for other agent implementations. The use of
     * consistent terminology for multiple agent implementations does perpetuate the use of
     * terminology that is inconsistent with custom ESDL transform terminology for the same concepts
     * (specifically, group versus profile and type versus option).
     *
     * Service delegation requires an implementation of the IAgent interface. This
     * implementation is obtained from a plugin-supplied CModuleFactory instance. Each
     * plugin is responsible for specifying which modules are available to the plugin, the labels
     * used to choose them, and which (if any) are chosen by default.
     */
    class LOGGINGCOMMON_API CEspLogAgent : implements IEspLogAgent, public CEspTraceLoggingComponent
    {
    public:
        constexpr static const char* propConfiguration = "@configuration";
    private:
        class CVariantIterator : public CEspLogAgentVariantIterator
        {
        protected:
            virtual const CEspLogAgentVariants& variants() const override { return m_agent->m_variants; }
            Linked<const CEspLogAgent> m_agent;
        public:
            CVariantIterator(const CEspLogAgent& agent) { m_agent.set(&agent); m_variantIt = variants().end(); }
        };
    public:
        IMPLEMENT_IINTERFACE;
        virtual const char* getName() override;
        virtual bool init(const char* name, const char* type, IPTree* configuration, const char* process) override;
        virtual bool initVariants(IPTree* configuration) override;
        virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response) override;
        virtual void getTransactionID(StringAttrMapping* fields, StringBuffer& id) override;
        virtual bool updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response) override;
        virtual bool hasService(LOGServiceType type) override;
        virtual IEspUpdateLogRequestWrap* filterLogContent(IEspUpdateLogRequestWrap* unfilteredRequest) override;
        virtual IEspLogAgentVariantIterator* getVariants() const override;
    private:
        Owned<const CModuleFactory> m_factory;
        StringBuffer m_name;
        Owned<ModularLogAgent::IAgent> m_agent;
        CEspLogAgentVariants m_variants;
        CLogContentFilter m_filter;
    public:
        CEspLogAgent(const CModuleFactory& m_factory);
    };

    /**
     * Common interface inherited by framework modules.
     *
     * Regardless of the logical purpose of a module, every module is expected to support a core
     * interface. The common interface addresses issues of module initialization, availability, and
     * trace log content generation.
     *
     * - All modules should use a common constructor interface. Technically not part of the module
     *   interface, the module factory assumes construction with a single argument - the module or
     *   log agent that contains it.
     * - All modules are initialized with a common interface.
     * - All modules are trace log sources, with independent control of message priority limits.
     * - All modules must provide an active or inactive state. This means that a module which may be
     *   inactive must be able to identify itself as inactive; it does not mean that all modules
     *   must support an inactive state.
     * - All modules must be able to produce a text representation of themselves as a debugging aid.
     */
    interface IModule : extends ITraceLoggingComponent
    {
        ///
        /// XPath of configuration property that informs the module factory which module
        /// implementation to instantiate.
        ///
        constexpr static const char* propFactoryKey         = "@module";

        /**
         * Initialize an instance from the configuration.
         *
         * Each instance initializes from a single property tree node. This is the node that
         * identified the specific implementation being initialized. An IAgent module will be
         * initialized using the moduleAgent element, while an IUpdateLog module will be
         * initialized using the moduleUpdateLog element.
         *
         * Modules that fail to initialize should not be relied upon, but it is the container's
         * decision as to how this is handled.
         *
         * @param configuration Required reference to a property tree node. Expectations for node
         *                      content are set by each implementation.
         * @param factory       Required reference to the log agent's module factory, enabling
         *                      modules to create nested modules.
         * @return True if an instance is successfully initialized, otherwise false.
         */
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) = 0;

        /**
         * Return the enabled state of the module.
         *
         * @return True if enabled, otherwise false.
         */
        virtual bool isEnabled() const = 0;

        /**
         * Add a text summary of the instance to the given text buffer.
         *
         * @param str The target buffer for the summary text.
         * @return Reference to the target buffer.
         */
        virtual StringBuffer& toString(StringBuffer& str) const = 0;
    };

    /**
     * Abstract representation of the LogAgent configuration element.
     *
     * The module accepts requests for each of LOGServiceType identified log agent service requests.
     * Every implementation must respond to all service requests but may choose which are supported
     * and which are no more than required stubs.
     */
    interface IAgent : extends IModule
    {
        /**
         * Provide access to the LOGServiceType::LGSTGetTransactionSeed log agent service.
         *
         * Service support is optional. A caller should not rely on the outcome unless
         * hasService(LGSTGetTransactionSeed) returns true.
         *
         * @param request  The seed request data as given to the log agent by the log manager.
         * @param response The outcome of the request.
         * @return True if a seed was obtained, otherwise false.
         */
        virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response) = 0;

        /**
         * Provide access to the LOGServiceType::LGSTGetTransactionID log agent service.
         *
         * Service support is optional. A caller should not rely on the outcome unless
         * hasService(LGSTGetTransactionID) returns true.
         *
         * @param transactionFields  Optional parameters used to construct an ID.
         * @param transactionId     Storage for a generated ID.
         */
        virtual void getTransactionID(StringAttrMapping* transactionFields, StringBuffer& transactionId) = 0;

        /**
         * Provide access to the LOGServiceType::LGSTUpdateLOG log agent service.
         *
         * Service support is optional. A caller should not rely on the outcome unless
         * hasService(LGSTUpdateLOG) returns true.
         *
         * @param request  The update request data as given to the log agent by the log manager.
         * @param response The outcome of the request.
         */
        virtual void updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response) = 0;

        /**
         * Inform the caller whether a requested log agent service is supported.
         *
         * Support is a combination of two factors. First, a subclass must include an implementation
         * of the service. Second, the implementation must not be disabled.
         *
         * @param type Log agent service identifier.
         * @return True if the instance can respond to a service request, otherwise false.
         */
        virtual bool hasService(LOGServiceType type) const = 0;
    };

    /**
     * Internal extension of the IAgent interface for use by implementations that delegate service
     * requests to separate modules.
     *
     * Each supported service is delegated to a unique service module interface. An implementation
     * will decide the number of service modules needed. One implementation might use one module per
     * service interface, while another might use one module for multiple services. This is an
     * implementation detail allowing developers to provide optimal solutions for their situatios.
     *
     * The LGSTUpdateLOG service is delegated to an IUpdateLog service module.
     */
    interface IDelegatingAgent : extends IAgent
    {
        virtual interface IUpdateLog* queryUpdateLog() const = 0;
    };

    /**
     * Placeholder interface to be extended by all modules used by IDelegatingAgent as delegates. As
     * the interfaces for these modules develop, any common interfaces may be declared here.
     */
    interface IServiceDelegate : extends IModule
    {
    };

    /**
     * Abstract representation of the UpdateLog configuration element, processing LGSTUpdateLOG
     * requests on behalf of an IDelegatingAgent. Implementations must accept a text representation
     * of an UpdateLogRequest XML document and report the outcome of the update.
     */
    interface IUpdateLog : extends IServiceDelegate
    {
        /**
         * Process an update log request.
         *
         * @param updateLogRequest Original request content, in text form. It is the caller's
         *                         responsibility to convert the log agent input into a string.
         * @param response         Output parameter updated by the implementation with the
         *                         outcome of the update. A status code of 0 indicates success.
         *                         A status code other than 0 indicates failure, and makes the
         *                         transaction eligible for a retry subject to the externally
         *                         configured retry rules.
         */
        virtual void updateLog(const char* updateLogRequest, IEspUpdateLogResponse& response) = 0;
    };

    /**
     * Internal extension of the IUpdateLog interface for use by implementations that delegate
     * aspects of update processing to separate modules.
     *
     * Similar in concept to IDelegatingAgent, it enables delegate modules to request information
     * from the delegating service. Unlike IDelegatingAgent, there is no immediately obvious
     * upgrade path requiring one delegate to communicate with another. It is, and will remain, an
     * empty placeholder until such communication is necessary.
     */
    interface IDelegatingUpdateLog : extends IUpdateLog
    {
    };

    /**
     * Abstract representation of the Target configuration element.
     *
     * An implementation is responsible for preserving log update content for future reference.
     * Each implementation defines its own method for content preservation. Examples might include
     * insertion of content into trace log output, writing to a file, inserting into a database, or
     * submitting SOAP requests to a remote web service.
     */
    interface IContentTarget : extends IModule
    {
        ///
        /// ESDL script context section containing the original log agent content.
        ///
        constexpr static const char* sectionOriginal = "original";
        ///
        /// ESDL script context section containing any results of custom ESDL transformations
        /// applied by the caller.
        ///
        constexpr static const char* sectionIntermediate = "intermediate";

        /**
         * Request data for one transaction to be preserved.
         *
         * Content intended for preservation is given in up to three different forms:
         *   -# The original content supplied to the log agent.
         *   -# The result of applying custom transforms to the original content.
         *   -# The result of applying an XSLT to either of the other content forms.
         *
         * Each subclass decides for itself which form or forms it will use. Where some may only
         * operate on the third form, others may act solely on the first. The configuration author
         * must ensure that each configured instance receives the required content form(s) from
         * the containing IDelegatingUpdateLog instance.
         *
         * The request outcome is reported using the response output parameter.
         *
         * @param scriptContext       Required ESDL script context enabling reuse between a
         *                            delegating module and its delegate. The first content form
         *                            can be accessed from sectionOriginal and the second form,
         *                            can be eccessed from sectionIntermediate.
         * @param originalContent     Required first content form.
         * @param intermediateContent Optional second content form. NULL suggests that no
         *                            custom transformations were applied.
         * @param finalContent        Optional third content form. NULL suggests no XSLT was
         *                            applied, while empty suggests there is no content to be
         *                            preserved.
         * @param response            Output parameter updated by the implementation with the
         *                            outcome of the update. A status code of 0 indicates success.
         *                            A status code other than 0 indicates failure, and makes the
         *                            transaction eligible for a retry subject to the externally
         *                            configured retry rules.
         */
        virtual void updateTarget(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, const char* finalContent, IEspUpdateLogResponse& response) const = 0;
    };

    /**
     * An external helper for CEspLogAgent. The agent requires an instance of the factory to be
     * given to it upon construction.
     *
     * The factory maintains registrations for each known type of module. A registration associates
     * a text label with a creation function that creates a concrete implementation of an abstract
     * module interface. When the configuration specifies a module label, the factory will create an
     * instance of the concrete type associated with the label. When no module label is given, the
     * label keyDefault is assumed..
     *
     * The factory initializes itself with all platform-defined modules. A plugin using only the built-
     * in modules may pass a default factory instance directly to the log agent. A plugin that wants
     * different default behavior, or that requires custom helpers, must update the registrations
     * as needed before creating the log agent.
     */
    class LOGGINGCOMMON_API CModuleFactory : public CInterface
    {
    public:
        enum Outcome
        {
            Added,
            Replaced,
            NotFound,
            Removed,
        };

        constexpr static const char* keyDefault = "default";
        constexpr static const char* keyFile = "file";

        /**
         * An association of text label to module creation function for a single module type. Template
         * parameters are the module interface implemented by a created interface and the object type
         * that contains it. The creation function takes a container instance as a parameter and
         * returns an implementation of the interface.
         */
        template <typename base_t, typename container_t>
        class TRegistrants
        {
        public:
            using Creator = std::function<base_t*(const container_t&)>;
            using Collection = std::map<std::string, Creator>;

            /**
             * Registers an association of the given text label with an auto-generated creation function
             * that creates instances of the method's template parameter type.
             */
            template <typename sub_t>
            Outcome add(const char* uid)
            {
                Outcome outcome = Added;
                if (isEmptyString(uid))
                    uid = keyDefault;
                WriteLockBlock block(m_rwLock);
                Creator& creator = m_registrants[uid];
                if (creator)
                    outcome = Replaced;
                creator = [](const container_t& container) { return new sub_t(container); };
                return outcome;
            }

            Outcome remove(const char* uid)
            {
                Outcome outcome = NotFound;
                if (isEmptyString(uid))
                    uid = keyDefault;
                WriteLockBlock block(m_rwLock);
                typename Collection::iterator it = m_registrants.find(uid);
                if (it != m_registrants.end())
                {
                    m_registrants.erase(it);
                    outcome = Removed;
                }
                return outcome;
            }

            bool create(Owned<base_t>& module, const IPTree* configuration, const container_t& container) const
            {
                bool result = true;
                if (configuration)
                {
                    using namespace TraceLoggingPriority;
                    const char* uid = configuration->queryProp(IModule::propFactoryKey);
                    if (isEmptyString(uid))
                        uid = keyDefault;
                    {
                        ReadLockBlock block(m_rwLock);
                        typename Collection::const_iterator it = m_registrants.find(uid);
                        if (m_registrants.end() == it)
                        {
                            container.uerrlog(Major, "unknown key (%s.%s)'", configuration->queryName(), uid);
                            result = false;
                        }
                        else if (!it->second)
                        {
                            container.ierrlog(Major, "invalid creator (%s.%s)", configuration->queryName(), uid);
                            result = false;
                        }
                        else
                        {
                            module.setown((it->second)(container));
                            if (!module)
                            {
                                container.ierrlog(Major, "creation failed (%s.%s)", configuration->queryName(), uid);
                                result = false;
                            }
                        }
                    }
                }
                else
                {
                    module.clear();
                }
                return result;
            }

        private:
            mutable ReadWriteLock m_rwLock;
            Collection            m_registrants;
        };

        using Agents = TRegistrants<ModularLogAgent::IAgent, CEspLogAgent>;
        using UpdateLogs = TRegistrants<ModularLogAgent::IUpdateLog, ModularLogAgent::IDelegatingAgent>;
        using ContentTargets = TRegistrants<ModularLogAgent::IContentTarget, ModularLogAgent::IDelegatingUpdateLog>;
        Agents              m_agents;
        UpdateLogs          m_updateLogs;
        ContentTargets      m_contentTargets;

        CModuleFactory();
    };

} // namespace ModularLogAgent

#endif // _MODULARLOGAGENT_HPP_
