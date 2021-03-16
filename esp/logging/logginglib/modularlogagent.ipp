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

#ifndef _MODULARLOGAGENT_IPP_
#define _MODULARLOGAGENT_IPP_

#include "modularlogagent.hpp"
#include "tokenserialization.hpp"
#include "xpathprocessor.hpp"

namespace ModularLogAgent
{
    class LOGGINGCOMMON_API CModule
    {
    public:
        constexpr static const char* propDisabled           = "@disabled";
        constexpr static const char* propName               = "@name";
        constexpr static const char* propTracePriorityLimit = "@trace-priority-limit";
    public:
        virtual LogMsgDetail tracePriorityLimit(const LogMsgCategory& category) const;
        virtual const char* traceId() const;
        virtual void traceOutput(const LogMsgCategory& category, const char* format, va_list& arguments) const  __attribute__((format(printf, 3, 0)));;
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory);
        virtual bool isEnabled() const;
        virtual StringBuffer& toString(StringBuffer& str) const;
    protected:
        mutable ReadWriteLock m_rwLock;
    private:
        template <typename container_t> friend class TModule;
        const ITraceLoggingComponent* m_self = nullptr;
        StringBuffer m_traceId;
        bool         m_disabled = false;
        bool         m_inheritTracePriorityLimit = true;
        LogMsgDetail m_tracePriorityLimit = TraceLoggingPriority::Major;
    protected:
        virtual bool appendProperties(StringBuffer& str) const;
        template <typename value_t>
        bool extract(const char* xpath, const IPTree& configuration, value_t& value, bool optional = true)
        {
            const char* property = configuration.queryProp(xpath);
            if (isEmptyString(property) && !optional)
            {
                if (m_self)
                    m_self->uerrlog(TraceLoggingPriority::Major, "missing required configuration property '%s'", xpath);
                return false;
            }
            DeserializationResult dr = TokenDeserializer().deserialize(property, value);
            switch (dr)
            {
            case Deserialization_SUCCESS:
                return true;
            default:
                return false;
            }
        }
    };

    template <typename container_t>
    class TModule : extends CModule
    {
    public:
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) override
        {
            const char* containerPrefix = m_container.traceId();
            if (!isEmptyString(containerPrefix))
                m_traceId.setf("%s.", containerPrefix);
            return CModule::configure(configuration, factory);
        }
        virtual LogMsgDetail tracePriorityLimit(const LogMsgCategory& category) const override
        {
            return (m_inheritTracePriorityLimit ? m_container.tracePriorityLimit(category) : CModule::tracePriorityLimit(category));
        }
    protected:
        using Container = container_t;
        const Container& m_container;
    public:
        TModule(const Container& container) : m_container(container) {}
    protected:
        const Container& queryContainer() const { return m_container; }

        /**
         * Provides for the creation and initialization of a delegate module. Delegates are not, by
         * default, required to be configured. An optional delegate omitted from the configuration
         * is acceptable, but the existence of a delegate configuration requires the configuration
         * to be valid even when the delegate is optional.
         */
        template <typename child_t, typename registrants_t, typename parent_t>
        bool createAndConfigure(const IPTree& configuration, const char* xpath, const CModuleFactory& factory, const registrants_t& registrants, Owned<child_t>& child, const parent_t& parent,  bool optional = true) const
        {
            const IPTree* node = configuration.queryBranch(xpath);
            if (!node)
            {
                if (optional)
                {
                    return true;
                }
                else
                {
                    if (m_self)
                        m_self->uerrlog(TraceLoggingPriority::Major, "required configuration node '%s' not found", xpath);
                }
            }
            else
            {
                return createAndConfigure(*node, factory, registrants, child, parent);
            }
            return false;
        }
        template <typename child_t, typename registrants_t, typename parent_t>
        bool createAndConfigure(const IPTree& node, const CModuleFactory& factory, const registrants_t& registrants, Owned<child_t>& child, const parent_t& parent) const
        {
            if (!registrants.create(child, &node, parent))
            {
                // create() has already reported the failure
            }
            else if (!child)
            {
                // should never happen, but just in case...
                if (m_self)
                    m_self->ierrlog(TraceLoggingPriority::Major, "factory creation of '%s' reported success but created no instance", node.queryName());
            }
            else if (!child->configure(node, factory))
            {
                // configure() has already reported the failure
                child.clear();
            }
            else
            {
                return true;
            }
            return false;
        }
    };

    class LOGGINGCOMMON_API CMockAgent : extends CSimpleInterfaceOf<IAgent>, extends TModule<CEspLogAgent>
    {
    public:
        using Base = TModule<CEspLogAgent>;
        IMPLEMENT_ITRACELOGGINGCOMPONENT_WITH(Base);
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) override;
        virtual bool isEnabled() const override { return true; }
        virtual StringBuffer& toString(StringBuffer& str) const override { return Base::toString(str); }
        virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response) override;
        virtual void getTransactionID(StringAttrMapping* fields, StringBuffer& id) override;
        virtual void updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response) override;
        virtual bool hasService(LOGServiceType type) const override;
    protected:
        Owned<IEspGetTransactionSeedResponse> m_gtsResponse;
        Owned<String> m_gtiResponse;
        Owned<IEspUpdateLogResponse> m_ulResponse;
    public:
        using Base::Base;
    };

    class LOGGINGCOMMON_API CDelegatingAgent : extends CSimpleInterfaceOf<IDelegatingAgent>, extends TModule<CEspLogAgent>
    {
    public:
        constexpr static const char* moduleUpdateLog = "UpdateLog";
        constexpr static const char* propUpdateLog   = "@UpdateLog";
    public:
        using Base = TModule<CEspLogAgent>;
        IMPLEMENT_ITRACELOGGINGCOMPONENT_WITH(Base);
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) override;
        virtual bool isEnabled() const override { return true; }
        virtual StringBuffer& toString(StringBuffer& str) const override { return Base::toString(str); }
        virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response) override;
        virtual void getTransactionID(StringAttrMapping* fields, StringBuffer& id) override;
        virtual void updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response) override;
        virtual bool hasService(LOGServiceType type) const override;
        virtual IUpdateLog* queryUpdateLog() const override { return m_updateLog; }
    protected:
        virtual bool configureUpdateLog(const IPTree& configuration, const CModuleFactory& factory);
        virtual bool appendProperties(StringBuffer& str) const override;
    protected:
        friend class CService;
        Owned<IUpdateLog>          m_updateLog;
    public:
        using Base::Base;
    };

    using CServiceDelegate = TModule<IDelegatingAgent>;

    class LOGGINGCOMMON_API CDelegatingUpdateLog : extends CSimpleInterfaceOf<IDelegatingUpdateLog>, extends CServiceDelegate
    {
    public:
        constexpr static const char* moduleTarget = "Target";
    public:
        using Base = CServiceDelegate;
        IMPLEMENT_ITRACELOGGINGCOMPONENT_WITH(Base);
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) override;
        virtual bool isEnabled() const override { return Base::isEnabled(); }
        virtual StringBuffer& toString(StringBuffer& str) const override { return Base::toString(str); }
        virtual void updateLog(const char* updateLogRequest, IEspUpdateLogResponse& response) override;
    protected:
        virtual bool appendProperties(StringBuffer& str) const override;
    protected:
        Owned<IContentTarget> m_target;
    public:
        using Base::Base;
    };

    using CUpdateLogDelegate = TModule<IDelegatingUpdateLog>;

    class LOGGINGCOMMON_API CContentTarget : extends CSimpleInterfaceOf<IContentTarget>, extends CUpdateLogDelegate
    {
    public:
        using Base = CUpdateLogDelegate;
        IMPLEMENT_ITRACELOGGINGCOMPONENT_WITH(Base);
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) override { return Base::configure(configuration, factory); }
        virtual bool isEnabled() const override { return Base::isEnabled(); }
        virtual StringBuffer& toString(StringBuffer& str) const override { return Base::toString(str); }
        virtual void updateTarget(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, const char* finalContent, IEspUpdateLogResponse& response) const override;
    public:
        using Base::Base;
    };

    /**
     * Concrete implementation of IContentTarget used to write text content to a file.
     *
     * By default, the third content form is written when not empty. The first content form is used
     * in this case to identify variable values that may be used to to construct a target filepath.
     * Enabling the optional debug mode writes all provided content forms to the file.
     *
     * The majority of file management code focuses on variable substitution. With XPath evaulation
     * readily available, it's natural to question why it isn't being used.
     *
     * Variable substitution in this module is a two step process. All variables except the creation
     * timestamp are substituted in step one. This yields a key pattern used to identify an already
     * open file that can accept the transaction data. If a new file is needed, the creation time-
     * stamp is substituted in the key pattern to yield a new filepath. XPath evaluation expects to
     * replace all values at once.
     *
     * Most variables are simple name-value substitutions, which XPath evaluation handles well. The
     * creation timestamp, however, is more complex. And it is more complex to satisfy internal
     * business requirements.
     */
    class LOGGINGCOMMON_API CFileTarget : public CContentTarget
    {
    public:
        using Variables = std::map<std::string, std::string>;

        class Pattern : public CInterface
        {
        private:
            interface IFragment : extends IInterface
            {
                virtual bool matches(const char* name, const char* option) const = 0;
                virtual bool resolvedBy(const Variables& variables) const = 0;
                virtual StringBuffer& toString(StringBuffer& pattern, const Variables* variables) const = 0;
                virtual IFragment* clone(const Pattern& pattern) const = 0;
            };

            struct Fragment : extends CSimpleInterfaceOf<IFragment>
            {
            protected:
                const Pattern& m_pattern;

                Fragment(const Pattern& pattern) : m_pattern(pattern) {};
            };

            struct TextFragment : extends Fragment
            {
                size32_t startOffset = 0;
                size32_t endOffset = 0;

                bool matches(const char* name, const char* option) const override { return false; }
                bool resolvedBy(const Variables& variables) const override { return startOffset < endOffset; }
                StringBuffer& toString(StringBuffer& pattern, const Variables* variables) const override { return pattern.append(endOffset - startOffset, m_pattern.m_textCache.str() + startOffset); }
                TextFragment* clone(const Pattern& pattern) const override { return nullptr; }

                TextFragment(const Pattern& pattern) : Fragment(pattern) {};
            };

            struct VariableFragment : extends Fragment
            {
                std::string m_name;
                std::string m_option;
                bool        m_withOption = false;

                bool matches(const char* name, const char* option) const override;
                bool resolvedBy(const Variables& variables) const override;
                StringBuffer& toString(StringBuffer& pattern, const Variables* variables) const override;
                VariableFragment* clone(const Pattern& pattern) const override;

                VariableFragment(const Pattern& pattern) : Fragment(pattern) {}
            };

            using Fragments = std::vector<Owned<IFragment> >;

            const CFileTarget& m_target;
            StringBuffer       m_textCache;
            Fragments          m_fragments;

        public:
            Pattern(const CFileTarget& target) : m_target(target) {}

            bool contains(const char* name, const char* option) const;
            void appendText(const char* begin, const char* end);
            bool setPattern(const char* pattern);
            Pattern* resolve(const Variables& variables) const;
            StringBuffer& toString(StringBuffer& output, const Variables* variables) const;
            inline StringBuffer& toString(StringBuffer& output) const { return toString(output, nullptr); }
        };

    protected:
        enum RolloverInterval
        {
            NoRollover,
            DailyRollover,
            UnknownRollover,
        };

        /**
         * Definition of an output file's data.
         */
        struct File : public CInterface
        {
            CDateTime m_lastWrite;   // time of last write to file
            StringBuffer m_filePath; // full path to file
            Owned<IFileIO> m_io;     // open file IO instance, ovtained from an untracked IFile
        };

        constexpr static const char* xpathHeader                     = "@header-text";
        constexpr static const char* xpathCreationDateFormat         = "@format-creation-date";
        constexpr static const char* defaultCreationDateFormat       = "%Y_%m_%d";
        constexpr static const char* xpathCreationTimeFormat         = "@format-creation-time";
        constexpr static const char* defaultCreationTimeFormat       = "%H_%M_%S";
        constexpr static const char* xpathCreationDateTimeFormat     = "@format-creation-datetime";
        constexpr static const char* defaultCreationDateTimeFormat   = "%Y_%m_%d_%H_%M_%S";
        constexpr static const char* xpathRolloverInterval           = "@rollover-interval";
        constexpr static const char* rolloverInterval_None           = "none";
        constexpr static const char* rolloverInterval_Daily          = "daily";
        constexpr static const char* defaultRolloverInterval         = rolloverInterval_Daily;
        constexpr static const char* xpathRolloverSize               = "@rollover-size";
        constexpr static const char* rolloverSize_None               = "0";
        constexpr static const char* defaultRolloverSize             = rolloverSize_None;
        constexpr static const char* xpathConcurrentFiles            = "@concurrent-files";
        constexpr static const char* concurrentFiles_None            = "1";
        constexpr static const char* defaultConcurrentFiles          = concurrentFiles_None;
        constexpr static const char* xpathFilePathPattern            = "@filepath";
        constexpr static const char* xpathDebug                      = "@debug";

        /**
         * File creation timestamp tokens. The variable name is defined by creationVarName. The format
         * string is defined, either directly or indirectly, by options:
         *   - creationVarDateOption: use m_creationDateFormat
         *   - creationVarTimeOption: use m_creationTimeFormat
         *   - creationVarDateTimeOption: use m_creationDateTimeFormat
         *   - creationVarCustomOption: use the variable value as a format string
         *   - any other value: use the option text as a format string
         */
        constexpr static const char* creationVarName                 = "creation";
        constexpr static const char* creationVarDateOption           = "date";
        constexpr static const char* creationVarTimeOption           = "time";
        constexpr static const char* creationVarDateTimeOption       = "datetime";
        constexpr static const char* creationVarCustomOption         = "custom";
        constexpr static const char* creationVarDefaultOption        = creationVarDateTimeOption;

        constexpr static const char* bindingVarName = "binding";
        constexpr static const char* processVarName = "process";
        constexpr static const char* portVarName = "port";
        constexpr static const char* esdlServiceVarName = "esdl-service";
        constexpr static const char* esdlMethodVarName = "esdl-method";
        constexpr static const char* serviceVarName = "service";

    public:
        virtual bool configure(const IPTree& configuration, const CModuleFactory& factory) override;
        virtual void updateTarget(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, const char* finalContent, IEspUpdateLogResponse& response) const override;
    protected:
        virtual bool appendProperties(StringBuffer& str) const override;

    protected:
        using TargetMap = std::map<std::string, Owned<File> >;
        Owned<Pattern>    m_pattern;
        StringBuffer      m_header;
        RolloverInterval  m_rolloverInterval = DailyRollover;
        offset_t          m_rolloverSize = 0;
        StringBuffer      m_creationDateFormat;
        StringBuffer      m_creationTimeFormat;
        StringBuffer      m_creationDateTimeFormat;
        uint8_t           m_concurrentFiles = 1;
        bool              m_debugMode = false;
        mutable TargetMap m_targets;

    public:
        using CContentTarget::CContentTarget;
    protected:
        virtual bool configureHeader(const IPTree& configuration);
        virtual bool configureCreationFormat(const IPTree& configuration, const char* xpath, const char* defaultValue, bool checkDate, bool checkTime, StringBuffer& format);
        virtual bool configureFileHandling(const IPTree& configuration);
        virtual bool configurePattern(const IPTree& configuration);
        virtual bool configureDebugMode(const IPTree& configuration);
        virtual void updateFile(const char* content, const Variables& variables, IEspUpdateLogResponse& response) const;
        virtual void readPatternVariables(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, Variables& variables) const;
        virtual bool validateVariable(const char* name, const char* option) const;
        virtual void resolveVariable(const char* name, const char* option, const char* value, StringBuffer& output) const;
        virtual File* getFile(const char* key) const;
        virtual bool needNewFile(size_t contentLength, const File& file, const Pattern& pattern, Variables& variables) const;
        virtual bool haveFile(const File& file) const;
        virtual bool createNewFile(File& file, const Pattern& pattern, Variables& variables) const;
        virtual bool writeChunk(File& file, offset_t pos, size32_t len, const char* data) const;
    };

} // namespace ModularLogAgent

#endif // _MODULARLOGAGENT_IPP_
