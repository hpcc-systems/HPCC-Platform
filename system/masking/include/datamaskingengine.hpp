/*##############################################################################

    Copyright (C) 2022 HPCC Systems®.

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

#include "jlog.hpp"
#include "jmutex.hpp"
#include "jstring.hpp"
#include "jutil.hpp"
#include "datamaskingshared.hpp"
#include "tracer.hpp"
#include <limits>
#include <list>

namespace DataMasking
{
    /**
     * @brief Load libraries and invoke entry points to create interface instances.
     *
     * Based on TPluginLoader from esp/platform/pluginloader.hpp.
     *
     * @tparam entry_point_t
     */
    template <typename entry_point_t>
    class TPluginLoader
    {
    public:
        template <typename instance_t>
        using Creator = std::function<instance_t*(entry_point_t entryPoint)>;

        TPluginLoader(const char* _libraryDefault, const char* _entryPointDefault, const char* _libraryXPath, const char* _entryPointXPath, ITracer& _tracer)
            : libraryXPath(_libraryXPath  ? _libraryXPath : "")
            , libraryDefault(_libraryDefault ? _libraryDefault : "")
            , entryPointXPath(_entryPointXPath ? _entryPointXPath : "")
            , entryPointDefault(_entryPointDefault ? _entryPointDefault : "")
        {
            tracer.set(&_tracer);
            std::string missing;
            if (libraryXPath.empty() && libraryDefault.empty())
                missing += "library name";
            if (entryPointXPath.empty() && entryPointDefault.empty())
            {
                if (!missing.empty())
                    missing += " and ";
                missing += "entryPoint function name";
            }
            if (!missing.empty())
                tracer->uerrlog("bad plugin loading configuration - missing %s", missing.c_str());
        }

        template <typename instance_t>
        instance_t* create(const IPTree& configuration, Creator<instance_t> creator)
        {
            instance_t* instance = nullptr;
            if (!creator)
            {
                tracer->uerrlog("plugin creation failed - invalid creation hook function");
                return nullptr;
            }
            else
            {
                try
                {
                    entry_point_t entryPoint = lookup(configuration);
                    if (entryPoint)
                        instance = creator(entryPoint);
                }
                catch (IException* e)
                {
                    StringBuffer msg;
                    tracer->uerrlog("plugin creation exception: %s", e->errorMessage(msg).str());
                }
                catch (...)
                {
                    tracer->uerrlog("plugin creation exception");
                }
            }
            return instance;
        }

        entry_point_t lookup(const IPTree& configuration)
        {
            const char* libraryName = nullptr;
            const char* entryPointName = nullptr;
            if (!libraryXPath.empty())
                libraryName = configuration.queryProp(libraryXPath.c_str());
            if (!entryPointXPath.empty())
                entryPointName = configuration.queryProp(entryPointXPath.c_str());
            return lookup(libraryName, entryPointName);
        }

        entry_point_t lookup(const char* libraryName, const char* entryPointName)
        {
            bool canFind = true;
            if (isEmptyString(libraryName))
            {
                if (libraryDefault.empty())
                {
                    tracer->uerrlog("plugin loader name lookup failed - no library name and no default value");
                    canFind = false;
                }
                else
                    libraryName = libraryDefault.c_str();
            }
            if (isEmptyString(entryPointName))
            {
                if (entryPointDefault.empty())
                {
                    tracer->uerrlog("plugin loader name lookup failed - no entry point name and no default value");
                    canFind = false;
                }
                else
                    entryPointName = entryPointDefault.c_str();
            }
            return (canFind ? find(libraryName, entryPointName) : nullptr);
        }

    private:
        using EntryPointMap = std::map<std::string, entry_point_t>;
        struct Plugin
        {
            HINSTANCE library = nullptr;
            EntryPointMap entryPoints;
        };
        using PluginMap = std::map<std::string, Plugin>;
        Linked<ITracer>        tracer;
        PluginMap              plugins;
        CriticalSection        lock;
        std::string            libraryXPath;
        std::string            libraryDefault;
        std::string            entryPointXPath;
        std::string            entryPointDefault;

        entry_point_t find(const char* libraryName, const char* entryPointName)
        {
            std::string realName;
            realName += SharedObjectPrefix;
            realName += libraryName;
            realName += SharedObjectExtension;
            CriticalBlock block(lock);
            Plugin& plugin = plugins[libraryName];
            if (!plugin.library)
            {
                plugin.library = LoadSharedObject(realName.c_str(), true, false);
                if (!plugin.library)
                {
                    tracer->uerrlog("plugin loader lookup failed - cannot load library '%s'", realName.c_str());
                    return nullptr;
                }
            }
            entry_point_t& entryPoint = plugin.entryPoints[entryPointName];
            if (!entryPoint)
            {
                entryPoint = (entry_point_t)GetSharedProcedure(plugin.library, entryPointName);
                if (!entryPoint)
                {
                    tracer->uerrlog("plugin loader lookup failed - library '%s' does not export '%s'", realName.c_str(), entryPointName);
                    return nullptr;
                }
            }
            return entryPoint;
        }
    };

    class CEngine : implements IDataMaskingEngine, implements IDataMaskingEngineInspector, public CInterface
    {
    protected:
        class Loader
        {
            Linked<ITracer>                            tracer;
            TPluginLoader<DataMaskingPluginEntryPoint> loader;
        public:
            Loader(ITracer& _tracer)
                : loader(nullptr, nullptr, "@library", "@entryPoint", _tracer)
            {
                tracer.set(&_tracer);
            }
            IDataMaskingProfileIterator* create(const IPTree& configuration)
            {
                Owned<IDataMaskingProfileIterator> profiles(loader.create<IDataMaskingProfileIterator>(configuration, [&](DataMaskingPluginEntryPoint entryPoint) { return entryPoint(configuration, *tracer); }));
                if (!profiles)
                {
                    class Empty : public CInterfaceOf<IDataMaskingProfileIterator>
                    {
                    public:
                        virtual bool first() override { return false; }
                        virtual bool next() override { return false; }
                        virtual bool isValid() override { return false; }
                        virtual IDataMaskingProfile& query() override { throw makeStringException(-1, "invalid iterator query"); }
                    };
                    profiles.setown(new Empty());
                    tracer->ierrlog("DataMasking::CEngine::Loader: configured plugin returned NULL");
                    if (tracer->dbglogIsActive())
                    {
                        StringBuffer xml;
                        toXML(&configuration, xml);
                        tracer->dbglog("DataMasking::CEngine::Loader: configuration\n%s", xml.str());
                    }
                }
                return profiles.getClear();
            }
        };
        class Domain : public CInterface
        {
        protected:
            struct less
            {
                bool operator () (const Owned<IDataMaskingProfile>& lhs, const Owned<IDataMaskingProfile>& rhs) const
                {
                    if (lhs && rhs)
                        return (lhs->inspector().queryMaximumVersion() < rhs->inspector().queryMaximumVersion());
                    if (lhs)
                        return true;
                    return false;
                }
            };
            using Content = std::set<Owned<IDataMaskingProfile>, less>;
            using ProfileIterator = TIteratorOfShared<IDataMaskingProfileIterator, Content::const_iterator>;
        protected:
            std::set<std::string>                      identifiers;
            std::set<Owned<IDataMaskingProfile>, less> profiles;
            DataMaskingVersionCoverage                 versions;
            bool                                       valid = false;
        public:
            const char* queryDefaultIdentifier() const
            {
                return (profiles.empty() ? nullptr : (*profiles.rbegin())->inspector().queryDefaultDomain());
            }
            uint8_t queryDefaultVersion() const
            {
                return (profiles.empty() ? 0 : (*profiles.rbegin())->inspector().queryDefaultVersion());
            }
            inline bool isValid() const
            {
                return valid;
            }
            bool checkId(const char* id) const
            {
                if (isEmptyString(id))
                    return false;
                return (identifiers.count(id) > 0);
            }
            bool checkIds(const IDataMaskingProfile& profile) const
            {
                Owned<ITextIterator> ids(profile.inspector().getAcceptedDomains());
                ForEach(*ids)
                {
                    if (identifiers.count(ids->query()))
                        return true;
                }
                return false;
            }
            bool checkVersion(uint8_t version) const
            {
                return (version ? versions.test(version) : false);
            }
            bool checkVersions(const IDataMaskingProfile& profile) const
            {
                const IDataMaskerInspector& inspector = profile.inspector();
                for (uint8_t idx = inspector.queryMinimumVersion(), lmt = inspector.queryMaximumVersion(); idx <= lmt; idx++)
                {
                    if (versions.test(idx))
                        return false;
                }
                return true;
            }
            void addProfile(IDataMaskingProfile& profile)
            {
                const IDataMaskerInspector& inspector = profile.inspector();
                Owned<ITextIterator> ids(inspector.getAcceptedDomains());
                ForEach(*ids)
                    identifiers.insert(ids->query());
                for (uint8_t idx = inspector.queryMinimumVersion(), lmt = inspector.queryMaximumVersion(); idx <= lmt; idx++)
                    versions.set(idx, true);
                profiles.emplace(LINK(&profile));
                if (profiles.size() == 1)
                    valid = inspector.isValid();
                else if (!inspector.isValid())
                    valid = false;
            }
            inline void getVersions(DataMaskingVersionCoverage& coverage) const
            {
                coverage |= versions;
            }
            IDataMaskingProfile* queryProfile(uint8_t version) const
            {
                if (!versions.test(version))
                    return nullptr;
                for (const Owned<IDataMaskingProfile>& profile : profiles)
                {
                    if (version <= profile->inspector().queryMaximumVersion())
                        return profile;
                }
                return nullptr;
            }
            ITextIterator* getIdentifiers() const
            {
                using SrcIterator = std::set<std::string>::const_iterator;
                using Iterator = TCachedTextIterator<SrcIterator>;
                return new Iterator(identifiers.begin(), identifiers.end(), StdStringExtractor<SrcIterator>());
            }
        };
        using Domains = std::list<Owned<Domain> >;
    public: // IInterface
        IMPLEMENT_IINTERFACE;
    public: // IDataMasker
        virtual const char* queryDomain() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->queryDomain() : nullptr);
        }
        virtual uint8_t queryVersion() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->queryVersion() : 0);
        }
        virtual uint8_t supportedOperations() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->supportedOperations() : 0);
        }
        virtual bool maskValue(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->maskValue(valueType, maskStyle, buffer, offset, length, conditionally) : false);
        }
        virtual bool maskContent(const char* contentType, char* buffer, size_t offset, size_t length) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->maskContent(contentType, buffer, offset, length) : false);
        }
        virtual bool maskMarkupValue(const char* element, const char* attribute, char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->maskMarkupValue(element, attribute, buffer, offset, length, callback) : false);
        }
    public: // IDataMaskingEngine
        virtual size_t loadProfiles(const IPTree& configuration) override
        {
            bool failed = false;
            size_t addedProfiles = 0;
            size_t addedDomains = 0;
            if (streq(configuration.queryName(), "maskingPlugin"))
            {
                if (!load(configuration, addedDomains, addedProfiles))
                    failed = true;
            }
            else
            {
                Owned<IPTreeIterator> pluginConfigs(configuration.getElements("//maskingPlugin"));
                ForEach(*pluginConfigs)
                {
                    if (!load(pluginConfigs->query(), addedDomains, addedProfiles))
                        failed = true;
                }
            }
            if (failed)
            {
                tracer->ierrlog("loadProfiles %s failed", (addedProfiles ? "partially" : "completely"));
                return std::numeric_limits<size_t>::max();
            }
            return addedProfiles;
        }
        virtual IDataMaskingProfile* queryProfile(const char* domainId, uint8_t version) const override
        {
            return selectProfile(domainId, version);
        }
        virtual IDataMaskingProfileContext* getContext(const char* domainId, uint8_t version, ITracer* tracer) const override
        {
            IDataMaskingProfile* profile = selectProfile(domainId, version);
            return (profile ? profile->createContext(version, tracer) : nullptr);
        }
        virtual const IDataMaskingEngineInspector& inspector() const override
        {
            return *this;
        }
    public: // IDataMaskerInspector
        virtual bool isValid() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().isValid() : false);
        }
        virtual const char* queryDefaultDomain() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().queryDefaultDomain() : nullptr);
        }
        virtual bool acceptsDomain(const char* id) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().acceptsDomain(id) : false);
        }
        virtual ITextIterator* getAcceptedDomains() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().getAcceptedDomains() : emptyText.getLink());
        }
        virtual uint8_t queryMinimumVersion() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().queryMinimumVersion() : 0);
        }
        virtual uint8_t queryMaximumVersion() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().queryMaximumVersion() : 0);
        }
        virtual uint8_t queryDefaultVersion() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().queryDefaultVersion() : 0);
        }
        virtual IDataMaskingProfileValueType* queryValueType(const char* name) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().queryValueType(name) : nullptr);
        }
        virtual bool hasValueType() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().hasValueType() : false);
        }
        virtual size_t countOfValueTypes() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().countOfValueTypes() : 0);
        }
        virtual IDataMaskingProfileValueTypeIterator* getValueTypes() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().getValueTypes() : nullptr);
        }
        virtual bool acceptsProperty(const char* name) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().acceptsProperty(name) : false);
        }
        virtual ITextIterator* getAcceptedProperties() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().getAcceptedProperties() : emptyText.getLink());
        }
        virtual bool usesProperty(const char* name) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().usesProperty(name) : false);
        }
        virtual ITextIterator* getUsedProperties() const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().getUsedProperties() : emptyText.getLink());
        }
        virtual bool hasRule(const char* contentType) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().hasRule(contentType) : false);
        }
        virtual size_t countOfRules(const char* contentType) const override
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().countOfRules(contentType) : 0);
        }
        virtual bool getMarkupValueInfo(const char* element, const char* attribute, const char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback, DataMaskingMarkupValueInfo& info) const
        {
            IDataMaskingProfile* profile = selectDefaultProfile();
            return (profile ? profile->inspector().getMarkupValueInfo(element, attribute, buffer, offset, length, callback, info) : false);
        }
    public: // IDataMaskingEngineInspector
        virtual size_t domainCount() const override
        {
            ReadLockBlock block(domainLock);
            return domains.size();
        }
        virtual bool hasDomain(const char* id) const override
        {
            if (isEmptyString(id))
                return false;
            ReadLockBlock block(domainLock);
            return (selectDomain(id) != nullptr);
        }
        virtual ITextIterator* getDomains() const override
        {
            using SrcIterator = Domains::const_iterator;
            using Iterator = TCachedTextIterator<SrcIterator>;
            struct Extractor
            {
                const char* value = nullptr;
                const char*& operator () (SrcIterator it)
                {
                    value = (*it)->queryDefaultIdentifier();
                    return value;
                }
            };
            ReadLockBlock block(domainLock);
            return new Iterator(domains.begin(), domains.end(), Extractor());
        }
        virtual ITextIterator* getDomainIds(const char* identifier) const override
        {
            ReadLockBlock block(domainLock);
            Domain* domain = selectDomain(identifier);
            return (domain ? domain->getIdentifiers() : emptyText.get());
        }
        virtual void getDomainVersions(const char* domainId, DataMaskingVersionCoverage& coverage) const override
        {
            if (!isEmptyString(domainId))
            {
                ReadLockBlock block(domainLock);
                Domain* domain = selectDomain(domainId);
                if (domain)
                    domain->getVersions(coverage);
            }
        }
    protected:
        Owned<ITracer>            tracer;
        Loader                    loader;
        std::list<Owned<Domain> > domains;
        mutable ReadWriteLock     domainLock;
        Owned<ITextIterator>      emptyText;
    public:
        CEngine()
            : tracer(ensureTracer(nullptr))
            , loader(*tracer)
        {
        }
        CEngine(ITracer* _tracer)
            : tracer(ensureTracer(_tracer))
            , loader(*tracer)
        {
            emptyText.setown(new EmptyScalarIteratorOf<const char*>());
        }
        inline IDataMaskingProfile* selectDefaultProfile() const
        {
            return selectProfile(nullptr, 0);
        }
        IDataMaskingProfile* selectProfile(const char* identifier, uint8_t version) const
        {
            ReadLockBlock block(domainLock);
            const Domain* domain = selectDomain(identifier);
            return (domain ? domain->queryProfile(version ? version : domain->queryDefaultVersion()) : nullptr);
        }
        Domain* selectDomain(const char* identifier) const
        {
            if (domains.empty())
                return nullptr;
            if (isEmptyString(identifier))
                return (domains.front()->isValid() ? domains.front() : nullptr);
            for (const Owned<Domain>& domain : domains)
            {
                if (domain->isValid() && domain->checkId(identifier))
                    return domain;
            }
            return nullptr;
        }
        bool load(const IPTree& configuration, size_t& addedDomains, size_t& addedProfiles)
        {
            bool failed = false;
            Owned<IDataMaskingProfileIterator> profiles(loader.create(configuration));
            WriteLockBlock block(domainLock);
            ForEach(*profiles)
            {
                IDataMaskingProfile& p = profiles->query();
                std::set<Domain*> domainMatches;
                for (Owned<Domain>& d : domains)
                {
                    if (d->checkIds(p))
                        domainMatches.emplace(d.get());
                }
                switch (domainMatches.size())
                {
                case 0:
                    domains.emplace_back(new Domain());
                    addedDomains++;
                    domains.back()->addProfile(p);
                    addedProfiles++;
                    break;
                case 1:
                    if ((*domainMatches.begin())->checkVersions(p))
                    {
                        (*domainMatches.begin())->addProfile(p);
                        addedProfiles++;
                    }
                    else
                    {
                        tracer->uerrlog("invalid domain %s version overlap", domains.front()->queryDefaultIdentifier());
                        failed = true;
                    }
                    break;
                default:
                    tracer->uerrlog("ambiguous domain identifiers");
                    failed = true;
                    break;
                }
            }
            return !failed;
        }
    private:
        static ITracer* ensureTracer(ITracer* given)
        {
            if (given)
                return given;
            return new CModularTracer();
        }
    };

} // namespace DataMasking
