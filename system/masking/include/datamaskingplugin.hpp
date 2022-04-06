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

#include "datamaskingshared.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include <algorithm>

namespace DataMasking
{
    /// Character sequence used to mask without an explicit mask style override.
    static const char* DefaultPattern = "*";

    /**
     * @brief Name assigned to a mapping of text identifiers to a version coverage bitset.
     *
     * Some profile constructs, such as profiles and value types, define keyed collections of other
     * profile constructs (profiles define value types; value types define mask styles). The keys
     * to these inner collections are required to be unique, but only per version. A profile may
     * define two instances of value type *foo* so long as the version ranges of each instance do
     * not overlap.
     *
     * An instance of this is used during configuration to track which versions are covered for
     * each unique key.
     */
    using CoverageMap = std::map<std::string, DataMaskingVersionCoverage>;

    /**
     * @brief Semi-concrete base class for profile constructs that supports versioning.
     *
     * Provided functionality includes:
     *
     * - Version range, including minimum and maximum versions
     * - Instance validity
     *
     * Subclass requirements include:
     *
     * - Default version (every instance must either specify or inherit a value)
     * - Instance identification (every instance should be able to identify itself as the source of
     *   trace output messages)
     *
     * Configuration:
     *
     * - @minimumVersion: Optional minimum version range value between 0 and 255. If constructed
     *                    without constraint, omitted, empty, or *0* defaults to *1*; with
     *                    constraint, omitted, empty, or *0* defaults to the constrained
     *                    minimum.
     * - @maximumVersion: Optional maximum version range value between 0 and 255. If constructed
     *                    without constraint, omitted, empty, or *0* defaults to the new
     *                    minimum (implying a version range dimension of 1); with constraint,
     *                    omitted, empty, or *0* defaults to the constrained maximum.
     */
    class Versioned
    {
    public: // abstract declarations
        virtual uint8_t         queryDefaultVersion() const = 0;
        virtual ITracer&        tracerRef() const = 0;
        virtual ITracer&        tracerRef(const IDataMaskingProfileContext* context) const = 0;
        virtual const char*     tracePrefix() const = 0;
    public:
        virtual bool        isValid() const {  return valid; }
        virtual uint8_t     queryMinimumVersion() const { return minimumVersion; }
        virtual uint8_t     queryMaximumVersion() const { return maximumVersion; }
        virtual bool        clipRange(uint8_t minimum, uint8_t maximum)
        {
            if (includes(minimum, maximum))
            {
                minimumVersion = minimum;
                maximumVersion = maximum;
                return true;
            }
            return false;
        }
        virtual void        update(DataMaskingVersionCoverage& coverage) const
        {
            for (uint8_t idx = minimumVersion; idx <= maximumVersion; idx++)
                coverage.set(idx);
        }
        virtual bool        configure(const IPTree& configuration)
        {
            bool defaultState = (!minimumVersion && !maximumVersion && valid);
            uint8_t newMin = readVersion(configuration, "@minimumVersion", (defaultState ? 1 : minimumVersion));
            uint8_t newMax = readVersion(configuration, "@maximumVersion", (defaultState ? newMin : maximumVersion));
            if (newMin && newMax)
            {
                if (defaultState)
                {
                    // accept new values
                }
                else if (minimumVersion && maximumVersion)
                {
                    if (includes(newMin, newMax))
                    {
                        // accept new values
                    }
                    else if (overlaps(newMin, newMax))
                    {
                        // pin values to current range
                        tracerRef().log(MCuserInfo, "%s: pinning %hhu..%hhu to %hhu..%hhu", tracePrefix(), newMin, newMax, minimumVersion, maximumVersion);
                        if (newMin < minimumVersion)
                            newMin = minimumVersion;
                        if (newMax > maximumVersion)
                            newMax = maximumVersion;
                    }
                    else
                    {
                        tracerRef().uerrlog("%s: range %hhu..%hhu not in range %hhu..%hhu", tracePrefix(), newMin, newMax, minimumVersion, maximumVersion);
                        valid = false;
                    }
                }
                else
                {
                    tracerRef().uerrlog("%s: range %hhu..%hhu not in range %hhu..%hhu", tracePrefix(), newMin, newMax, minimumVersion, maximumVersion);
                    valid = false;
                }
                if (newMin > newMax)
                {
                    tracerRef().uerrlog("%s: invalid version range %hhu..%hhu", tracePrefix(), newMin, newMax);
                    valid = false;
                }
            }
            else
            {
                // error(s) reported by readVersion
            }
            minimumVersion = newMin;
            maximumVersion = newMax;
            return valid;
        }
        inline bool         overlaps(uint8_t min, uint8_t max) const { return ((min <= maximumVersion) && (max >= minimumVersion)); }
        inline bool         overlaps(const Versioned& test) const { return overlaps(test.minimumVersion, test.maximumVersion); }
        inline bool         includes(uint8_t min, uint8_t max) const { return ((min <= max) && (minimumVersion <= min) && (max <= maximumVersion)); }
        inline bool         includes(const Versioned& test) const { return includes(test.minimumVersion, test.maximumVersion); }
        inline bool         includes(uint8_t version) const { return ((minimumVersion <= version) && (version <= maximumVersion)); }
    protected:
        uint8_t minimumVersion = 0;
        uint8_t maximumVersion = 0;
        bool    valid = true;
    public:
        /**
         * @brief Construct a new Versioned object with the same range as the optional constaint.
         *
         * This is not intended to be a copy constructor. It does copy the input range but validity
         * is determined solely on the validity of the range. An invalid constraint may yield a
         * valid instance.
         *
         * @param constraint
         */
        Versioned(const Versioned* constraint)
        {
            if (constraint)
            {
                minimumVersion = constraint->minimumVersion;
                maximumVersion = constraint->maximumVersion;
                valid = (minimumVersion && maximumVersion && (minimumVersion <= maximumVersion));
            }
        }
        virtual ~Versioned() {}
    protected:
        /**
         * @brief Read a version value from the configuration.
         *
         * Gets the requested version number from the configuration, or invalidates `this`. The
         * configuration's `getPropInt` and similar methods are not used to enable improved error
         * detection and reporting.
         *
         * A default value is returned when the requested value is omitted, empty, whitespace, or
         * *0*. The configured number is returned when numeric conversion is successful. *0* is
         * returned when numeric conversion fails, and the instance is invalidated.
         *
         * @param configuration
         * @param xpath
         * @param defaultValue
         * @return uint8_t
         */
        uint8_t readVersion(const IPTree& configuration, const char* xpath, uint8_t defaultValue)
        {
            const char* propValue = skipSpace(configuration.queryProp(xpath));
            if (isEmptyString(propValue))
                return defaultValue;
            char* endptr = nullptr;
            unsigned long tmp = strtoul(propValue, &endptr, 0);
            if (endptr == propValue)
            {
                tracerRef().uerrlog("%s: invalid %s value '%s'; expected a number", tracePrefix(), xpath, propValue);
                valid = false;
                return 0;
            }
            else if (!isEmptyString(skipSpace(endptr)))
            {
                tracerRef().uerrlog("%s: invalid %s value '%s'; unexpected non-numeric characters", tracePrefix(), xpath, propValue);
                valid = false;
                return 0;
            }
            else if (std::numeric_limits<uint8_t>::max() < tmp)
            {
                tracerRef().uerrlog("%s: invalid %s value '%s'; expected number in range 0..%hhu", tracePrefix(), xpath, propValue, std::numeric_limits<uint8_t>::max());
                valid = false;
                return 0;
            }
            else if (0 == tmp)
            {
                return defaultValue;
            }
            return uint8_t(tmp);
        }
    private:
        const char* skipSpace(const char* ptr) const
        {
            if (ptr)
            {
                while (isspace(*ptr))
                    ptr++;
            }
            return ptr;
        }
    };

    /**
     * @brief Version coverage of a custom property.
     *
     * Properties may either be accepted (i.e., known by a profile construct) or used (referenced
     * by a profile construct). Acceptance does not imply use, but use does imply acceptance.
     *
     * This tracks both acceptance and usage of a single named property.
     */
    struct PropertyCoverages
    {
        DataMaskingVersionCoverage accepted;
        DataMaskingVersionCoverage used;

        void accept(const Versioned& range)
        {
            merge(range, accepted);
        }
        void use(const Versioned& range)
        {
            merge(range, accepted);
            merge(range, used);
        }
    protected:
        inline void merge(const Versioned& range, DataMaskingVersionCoverage& coverage)
        {
            range.update(coverage);
        }
    };

    /**
     * @brief Name assigned to a mapping of custom property name to property coverage.
     *
     * A profile may declare acceptance of custom properties without using any. Other profile
     * constructs might declare usage of properties not known to the profile. Rather than
     * traversing all profile constructs searching for acceptance or usage, each construct
     * updates a profile-defined map with names and versions. This optimizes profile interrogation
     * times in time-sensitive situations.
     */
    using PropertyCoverageMap = std::map<std::string, PropertyCoverages>;

    /**
     * @brief Extension of `Versioned` that adds data and behaviors common shared by the profile
     *        constructs created by a profile.
     *
     * Overridden functionality includes:
     *
     * - The default version is defined as the containing construct's default version
     * - The instance identifies as `container-identity "/" configuration-element-name "(" ( name | "<unnamed>" )
     *
     * Provided functionality includes:
     *
     * - A name token may be configured. Each subclass determines whether this value is required
     *   or optional.
     * - Context compatibility requires validity and version support.
     *
     * Configuration:
     *
     * - @name: Identification token. May be omitted or empty for subclasses that permit it.
     */
    class CProfileEntity : public Versioned
    {
    public: // Versioned implementations and overrides
        virtual uint8_t queryDefaultVersion() const override
        {
            return (container ? container->queryDefaultVersion() : 0);
        }
        virtual bool configure(const IPTree& configuration) override
        {
            // Update the instance identity first, so it is available during base class
            // configuration.
            const char* newName = configuration.queryProp("@name");
            if (!isEmptyString(newName))
            {
                name = newName;
                updatePath(configuration);
            }
            else if (nameRequired)
            {
                updatePath(configuration);
                tracerRef().ierrlog("%s: missing required name", this->tracePrefix());
                valid = false;
            }
            else
            {
                updatePath(configuration);
            }
            return Versioned::configure(configuration);
        }
        virtual ITracer& tracerRef() const override
        {
            if (container)
                return container->tracerRef();
            throw makeStringExceptionV(-1, "%s: invalid profile entity - missing container", tracePrefix());
        }
        virtual ITracer& tracerRef(const IDataMaskingProfileContext* context) const override
        {
            if (container)
                return container->tracerRef(context);
            throw makeStringExceptionV(-1, "%s: invalid profile entity - missing container", tracePrefix());
        }
        virtual const char* tracePrefix() const override
        {
            return path.c_str();
        }
    public: // IDataMaskingProfileEntity base implementations
        virtual const char* queryName() const
        {
            return name.c_str();
        }
        virtual bool matches(const IDataMaskingProfileContext* context) const
        {
            return (isValid() && includes(context ? context->queryVersion() : queryDefaultVersion()));
        }
    protected:
        const Versioned*  container = nullptr;
        std::string       name;
        std::string       path;
    private:
        bool              nameRequired = true;
    public:
        CProfileEntity(const Versioned& _container, bool _nameRequired)
            : Versioned(&_container)
            , container(&_container)
            , nameRequired(_nameRequired)
        {
        }
    protected:
        void updatePath(const IPTree& configuration)
        {
            path += container->tracePrefix();
            path += "/";
            path += configuration.queryName();
            path += "(";
            path += (name.empty() ? "<unnamed>" : name.c_str());
            path += ")";
        }
    };

    /**
     * @brief Profile entity that implements `IDataMaskingProfileMaskStyle`.
     *
     * Extended functionality includes:
     *
     * - Obfuscation of a range of text using a masking pattern.
     *
     * Configuration:
     *
     * - @minimumVersion:  Optional. See `Versioned`.
     * - @maximumVersion:  Optional. See `Versioned`.
     * - @name:            Required. See `CProfileEntity`
     * - @pattern:         Optional character sequence used to mask text. Affected characters are
     *                     replaced by asterisks if omitted or empty.
     * - @overrideDefault: Optional Boolean flag indicating whether the instance is a replacement
     *                     for the default mask style of the containing value type. False if
     *                     omitted or empty.
     */
    class CMaskStyle : public CInterfaceOf<IDataMaskingProfileMaskStyle>, public CProfileEntity
    {
    public:
        virtual const char* queryName() const override { return CProfileEntity::queryName(); }
        virtual uint8_t     queryMinimumVersion() const override { return CProfileEntity::queryMinimumVersion(); }
        virtual uint8_t     queryMaximumVersion() const override { return CProfileEntity::queryMaximumVersion(); }
        virtual bool        matches(const IDataMaskingProfileContext* context) const override { return CProfileEntity::matches(context); }
        virtual bool        applyMask(const IDataMaskingProfileContext* context, char* buffer, size_t offset, size_t length) const override
        {
            return maskWithPattern(buffer, offset, length, pattern.c_str());
        }
    protected:
        std::string pattern;
        bool        overridesDefault = false;
    public:
        CMaskStyle(const Versioned& _container)
            : CProfileEntity(_container, true)
            , pattern(DefaultPattern)
        {
        }
        virtual bool configure(const IPTree& configuration, PropertyCoverageMap& propertyCoverages)
        {
            CProfileEntity::configure(configuration);
            const char* newPattern = configuration.queryProp("@pattern");
            if (newPattern)
            {
                if (!*newPattern)
                    tracerRef().uwarnlog("%s: ignoring empty mask style pattern", this->tracePrefix());
                else
                    pattern = newPattern;
            }
            overridesDefault = configuration.getPropBool("@overrideDefault");
            return valid;
        }
        virtual bool isDefault() const
        {
            return overridesDefault;
        }
        static bool maskWithPattern(char* buffer, size_t offset, size_t length, const char* pattern)
        {
            if (isEmptyString(buffer) || (0 == length) || isEmptyString(pattern))
            {
                return false;
            }
            else if (offset && isEmptyString(buffer += offset))
            {
                return false;
            }
            else if (std::numeric_limits<size_t>::max() == length)
            {
                const char* tmp = pattern;
                while (buffer[offset])
                {
                    buffer[offset++] = *tmp++;
                    if (!*tmp)
                        tmp = pattern;
                }
            }
            else
            {
                const char* tmp = pattern;
                for (size_t bufferIdx = offset, limit = offset + length; bufferIdx < limit; bufferIdx++)
                {
                    buffer[bufferIdx] = *tmp++;
                    if (!*tmp)
                        tmp = pattern;
                }
            }
            return true;
        }
    };

    /**
     * @brief Name assigned to a collection of mask styles.
     *
     * A multimap keyed by mask style name is used. Name collisions are possible, and are resolved
     * by version checks (i.e., for each name, only one instance is permitted per version).
     *
     * See `CoverageMap`.
     *
     * @tparam maskstyle_t
     */
    template <typename maskstyle_t>
    using MaskStyleMap = std::multimap<std::string, Owned<maskstyle_t> >;

    /**
     * @brief Name assigned to a collection of unique set names.
     */
    using SetMemberships = std::set<std::string>;

    /**
     * @brief Base extension of profile entity representing a rule. With this implementation,
     *        in-memory representations of rules may exist but they cannot be applied in a mask
     *        operation.
     *
     * Overridden functionality includes:
     *
     * - Context compatibility also ensures proper set membership and that the containing
     *   value type is also compatible.
     *
     * Basic functionality includes:
     *
     * - Set membership. A rule set is a collection of rules, possibly spanning multiple value
     *   types, that are applied together. Every rule belongs to at least one set. There is an
     *   implied, unnamed set to which all rules belong by default. A rule assigned to a named
     *   set may also belong to the unnamed set, but this membership must be explicitly defined.
     *
     * Configuration:
     *
     * - @minimumVersion:  Optional. See `Versioned`.
     * - @maximumVersion:  Optional. See `Versioned`.
     * - @name:            Optional. See `CProfileEntity`
     * - memberOf:         Optional, repeating rule set membership element. If omitted, a rule
     *                     belongs to the unnamed set. If present, the rule belongs only to the
     *                     explicitly configured sets.
     * - memberOf/@name:   Optional set name. Omitted or empty implies unnamed set membership.
     *
     * Usage:
     *
     * - Omission of a custom context property named *rule-set* matches the unnamed rule set.
     * - An empty value for a custom context property named *rule-set* matches the unnamed
     *   rule set.
     * - A value of \* for a custom context property named *rule-set* matches all rules,
     *   regardless of set membership.
     * - Any other value for a custom context property named *rule-set* matches only those rules
     *   that are explicitly configured to belong to that set.
     */
    class CRule : public CInterface, public CProfileEntity
    {
    public:
        virtual bool matches(const IDataMaskingProfileContext* context) const override
        {
            if (CProfileEntity::matches(context) && valueType.matches(context))
            {
                const char* requestedSet = (context ? context->queryProperty(DataMasking_RuleSet) : nullptr);
                if (isEmptyString(requestedSet))
                    return (memberships.empty() || memberships.count("") != 0);
                if (streq(requestedSet, DataMasking_AnySet))
                    return true;
                return (memberships.count(requestedSet) != 0);
            }
            return false;
        }
    protected:
        const IDataMaskingProfileValueType&  valueType;
        SetMemberships memberships;
        std::string contentType;
    public:
        CRule(const Versioned& container, const IDataMaskingProfileValueType& _valueType)
            : CProfileEntity(container, false)
            , valueType(_valueType)
        {
        }
        virtual bool configure(const IPTree& configuration, PropertyCoverageMap& propertyCoverages)
        {
            CProfileEntity::configure(configuration);
            if (!configureSetMembership(configuration, propertyCoverages))
                valid = false;
            if (!configureContentType(configuration))
                valid = false;
            return valid;
        }

        inline const SetMemberships& queryMemberships() const
        {
            return memberships;
        }
        inline const char* queryContentType() const
        {
            return contentType.c_str();
        }
        inline const IDataMaskingProfileValueType& queryValueType() const
        {
            return valueType;
        }
    protected:
        virtual bool configureSetMembership(const IPTree& configuration, PropertyCoverageMap& propertyCoverages)
        {
            bool explicitUnnamed = false;
            Owned<IPTreeIterator> it(configuration.getElements("memberOf"));
            ForEach(*it)
            {
                const char* name = it->query().queryProp("@name");
                if (isEmptyString(name))
                    explicitUnnamed = true;
                else
                    memberships.insert(name);
            }
            if (explicitUnnamed && !memberships.empty())
                memberships.insert("");
            if (!memberships.empty() || explicitUnnamed)
                propertyCoverages[DataMasking_RuleSet].use(*this);
            for (const std::string& m : memberships)
            {
                std::string tmp(DataMasking_RuleSet);
                tmp += ":";
                tmp += m;
                propertyCoverages[tmp.c_str()].use(*this);
            }
            return true;
        }
        virtual bool configureContentType(const IPTree& configuration)
        {
            const char* tmp = configuration.queryProp("@contentType");
            if (!isEmptyString(tmp))
                contentType = tmp;
            return true;
        }
    };

    /**
     * @brief Name assigned to a collection of rules.
     *
     * A list is sufficient because they are not referenced by name.
     *
     * @tparam rule_t
     */
    template <typename rule_t>
    using RuleList = std::list<Owned<rule_t> >;

    /**
     * @brief Profile entity that implements `IDataMaskingProfileValueType`
     *
     * Overridden functionality includes:
     *
     * - Context compatibility also ensures proper set membership.
     *
     * Extended functionality includes:
     *
     * - Set membership. A value type set is a subset of the value types defined in a profile
     *   that are enabled or disabled together. Every value type belongs to at least one set.
     *   There is an implied, unnamed set to which all value types belong by default. A value
     *   type assigned to a named set does not belong to the unnamed set unless this
     *   membership is explicitly defined.
     * - Default masking implementation, without use of custom mask styles.
     * - Creation of custom mask styles that can be applied by name.
     * - Creation of custom mask styles that can override the default behavior.
     * - Creation of rules for use with *mask content* and *mask markup value* requests.
     * - Character obfuscation through built in and custom masking.
     *
     * Configuration:
     *
     * - @minimumVersion:  Optional. See `Versioned`.
     * - @maximumVersion:  Optional. See `Versioned`.
     * - @name:             Required. See `CProfileEntity`
     * - memberOf:          Optional, repeating rule set membership element. If omitted, a value
     *                      type belongs to the unnamed set. If present, the value type belongs
     *                      only to the explicitly configured sets.
     * - memberOf/@name:    Optional set name. Omitted or empty implies unnamed set membership.
     * - maskStyle:         Optional. See the definition of `maskstyle_t` for more information.
     * - rule:              Optional. See the definition of `rule_t` for more information.
     *
     * Usage:
     *
     * - Omission of a custom context property named *valuetype-set* matches the unnamed value
     *   type set.
     * - An empty value for a custom context property named *valuetype-set* matches the unnamed
     *   value type set.
     * - A value of \* for a custom context property named *valuetype-set* matches all value
     *   types, regardless of set membership.
     * - Any other value for a custom context property named *valuetype-set* matches the value
     *   types in both the unnamed set and the named set.
     *
     * @tparam maskstyle_t
     * @tparam rule_t
     */
    template <typename maskstyle_t, typename rule_t>
    class TValueType : public CInterfaceOf<IDataMaskingProfileValueType>, public CProfileEntity
    {
    public:
        using Self = TValueType<maskstyle_t, rule_t>;
        struct MaskStyleInfo
        {
            DataMaskingVersionCoverage      coverage;
            std::list<Linked<maskstyle_t> > styles;
        };
    public:
        virtual const char* queryName() const override { return CProfileEntity::queryName(); }
        virtual uint8_t     queryMinimumVersion() const override { return CProfileEntity::queryMinimumVersion(); }
        virtual uint8_t     queryMaximumVersion() const override { return CProfileEntity::queryMaximumVersion(); }
        virtual bool        matches(const IDataMaskingProfileContext* context) const override
        {
            if (CProfileEntity::matches(context))
            {
                if (memberships.empty())
                    return true;
                const char* requestedSet = (context ? context->queryProperty(DataMasking_ValueTypeSet) : nullptr);
                if (isEmptyString(requestedSet))
                    return false;
                if (streq(requestedSet, DataMasking_AnySet))
                    return true;
                return (memberships.count(requestedSet) != 0);
            }
            return false;
        }
        virtual bool isMemberOf(const char* name) const override
        {
            if (isEmptyString(name))
                return (memberships.empty() || memberships.count("") != 0);
            return (memberships.count(name) != 0);
        }
        virtual maskstyle_t* queryMaskStyle(const IDataMaskingProfileContext* context, const char* name) const override
        {
            if (isEmptyString(name))
                return nullptr;
            std::pair<typename MaskStyleMap<maskstyle_t>::const_iterator, typename MaskStyleMap<maskstyle_t>::const_iterator> range = maskStyles.equal_range(name);
            if (range.first != maskStyles.end())
            {
                unsigned checkVersion = (context ? context->queryVersion() : queryDefaultVersion());
                for (typename MaskStyleMap<maskstyle_t>::const_iterator it = range.first; it != range.second; ++it)
                {
                    if (it->second->matches(context))
                    {
                        return (it->second->isValid() ? it->second.get() : nullptr);
                    }
                }
            }
            return nullptr;
        }
        virtual IDataMaskingProfileMaskStyleIterator* getMaskStyles(const IDataMaskingProfileContext* context) const override
        {
            using SrcIterator = typename MaskStyleMap<maskstyle_t>::const_iterator;
            using MaskStyleIterator = TIteratorOfShared<IDataMaskingProfileMaskStyle, SrcIterator>;
            struct Filter
            {
                const IDataMaskingProfileContext* context;
                bool operator () (SrcIterator it) const { return it->second->matches(context); }
                Filter(const IDataMaskingProfileContext* _context) : context(_context) {}
            };
            return new MaskStyleIterator(maskStyles.begin(), maskStyles.end(), extractMappedShared<IDataMaskingProfileMaskStyle, SrcIterator>, Filter(context));
        }
        virtual bool applyMask(const IDataMaskingProfileContext* context, const char* maskStyle, char* buffer, size_t offset, size_t length) const override
        {
            if (isEmptyString(maskStyle) || streq(maskStyle, "default"))
                return applyDefaultMask(context, buffer, offset, length);
            maskstyle_t* ms = queryMaskStyle(context, maskStyle);
            if (ms)
                return ms->applyMask(context, buffer, offset, length);
            {
                // For debugging purposes, report erroneous mask style requests once per name.
                std::string key(maskStyle);
                CriticalBlock block(badMasksLock);
                if (badMasks.count(key) == 0)
                {
                    if (maskStyles.count(key) == 0)
                        tracerRef(context).iwarnlog("%s: invalid request for undefined mask style '%s'; applying default mask", tracePrefix(), maskStyle);
                    else
                        tracerRef(context).iwarnlog("%s: unable to use mask style '%s'; applying default mask", tracePrefix(), maskStyle);
                    badMasks.insert(key);
                }
            }
            return applyDefaultMask(context, buffer, offset, length);
        }
    protected:
        SetMemberships                memberships;
        MaskStyleMap<maskstyle_t>     maskStyles;
        MaskStyleInfo                 defaultMask;
        mutable std::set<std::string> badMasks;
        mutable CriticalSection       badMasksLock;
    public:
        TValueType(const Versioned& _container)
            : CProfileEntity(_container, true)
        {
        }
        virtual bool configure(const IPTree& configuration, PropertyCoverageMap& propertyCoverageMap, RuleList<rule_t>& profileRules)
        {
            if (!CProfileEntity::configure(configuration))
                valid = false;
            if (!configureSetMembership(configuration, propertyCoverageMap))
                valid = false;
            if (!configureMaskStyles(configuration, propertyCoverageMap))
                valid = false;
            if (!configureRules(configuration, propertyCoverageMap, profileRules))
                valid = false;
            return valid;
        }
        virtual bool applyMask(char* buffer, size_t offset, size_t length, const char* pattern) const
        {
            return CMaskStyle::maskWithPattern(buffer, offset, length, pattern);
        }
        inline const SetMemberships& queryMemberships() const
        {
            return memberships;
        }
    protected:
        virtual bool configureSetMembership(const IPTree& configuration, PropertyCoverageMap& propertyCoverages)
        {
            Owned<IPTreeIterator> it(configuration.getElements("memberOf"));
            ForEach(*it)
            {
                const char* name = it->query().queryProp("@name");
                if (!isEmptyString(name))
                    memberships.insert(name);
            }
            if (memberships.empty())
            {
                propertyCoverages[DataMasking_ValueTypeSet].accept(*this);
            }
            else
            {
                propertyCoverages[DataMasking_ValueTypeSet].use(*this);
                for (const std::string& m : memberships)
                {
                    std::string tmp(DataMasking_ValueTypeSet);
                    tmp += ":";
                    tmp += m;
                    propertyCoverages[tmp.c_str()].use(*this);
                }
            }
            return true;
        }
        virtual bool configureMaskStyles(const IPTree& configuration, PropertyCoverageMap& propertyCoverageMap)
        {
            bool invalidated = false;
            CoverageMap coverages;
            Owned<IPTreeIterator> it(configuration.getElements("maskStyle"));
            ForEach(*it)
            {
                Owned<maskstyle_t> maskStyle(createMaskStyle(it->query(), propertyCoverageMap));
                if (!maskStyle)
                {
                    invalidated = true;
                    continue;
                }
                const char* key = maskStyle->queryName();
                if (isEmptyString(key))
                {
                    invalidated = true;
                    continue;
                }
                DataMaskingVersionCoverage& coverage = coverages[key];
                if (coverage.any())
                {
                    DataMaskingVersionCoverage tmp;
                    maskStyle->update(tmp);
                    tmp &= coverage;
                    if (tmp.any())
                    {
                        tracerRef().uerrlog("%s: invalid version coverage for mask style '%s'", this->tracePrefix(), key);
                        invalidated = true;
                    }
                }
                if (maskStyle->isDefault())
                {
                    DataMaskingVersionCoverage tmp;
                    maskStyle->update(tmp);
                    tmp &= defaultMask.coverage;
                    if (tmp.any())
                    {
                        tracerRef().uerrlog("%s: invalid default version coverage", this->tracePrefix());
                        invalidated = true;
                    }
                    defaultMask.styles.emplace_back(maskStyle.get());
                    maskStyle->update(defaultMask.coverage);
                }
                maskStyle->update(coverage);
                maskStyles.emplace(key, maskStyle.getClear());
            }
            return !invalidated;
        }
        virtual bool configureRules(const IPTree& configuration, PropertyCoverageMap& propertyCoverageMap, RuleList<rule_t>& profileRules)
        {
            bool invalidated = false;
            Owned<IPTreeIterator> it(configuration.getElements("rule"));
            ForEach(*it)
            {
                Owned<rule_t> rule(createRule(it->query(), propertyCoverageMap));
                if (rule)
                    profileRules.emplace_back(rule.getClear());
                else
                    invalidated = true;
            }
            if (invalidated)
                tracerRef().uerrlog("%s: invalid due to invalid rules", this->tracePrefix());
            return !invalidated;
        }
        virtual maskstyle_t* createMaskStyle(const IPTree& configuration, PropertyCoverageMap& propertyCoverageMap)
        {
            Owned<maskstyle_t> ms(createMaskStyle());
            if (!ms->configure(configuration, propertyCoverageMap))
                ms.clear();
            return ms.getClear();
        }
        virtual maskstyle_t* createMaskStyle()
        {
            return new maskstyle_t(*this);
        }
        virtual rule_t* createRule(const IPTree& configuration, PropertyCoverageMap& propertyCoverageMap)
        {
            Owned<rule_t> rule(createRule());
            if (!rule->configure(configuration, propertyCoverageMap))
                rule.clear();
            return rule.getClear();
        }
        virtual rule_t* createRule()
        {
            return new rule_t(*this, *this);
        }
        virtual bool applyDefaultMask(const IDataMaskingProfileContext* context, char* buffer, size_t offset, size_t length) const
        {
            uint8_t version = (context ? context->queryVersion() : queryDefaultVersion());
            if (defaultMask.coverage.test(version))
            {
                for (const Linked<maskstyle_t>& s : defaultMask.styles)
                {
                    if (s->includes(version, version))
                        return s->applyMask(context, buffer, offset, length);
                }
                tracerRef(context).ierrlog("%s: default mask coverage mismatch", tracePrefix());
            }
            return applyMask(buffer, offset, length, DefaultPattern);
        }
    };

    /**
     * @brief An abstract profile implementation supporting domain identity, versioning, and
     *        tracking supported properties (both accepted and used).
     *
     * All requests that could be contextual are routed to virtual methods that expect a context
     * reference as input. A default immutable context instance is used by the class to satisfy
     * requests made through the `IDataMaskingProfile` and  `IDataMaskingEngine` interfaces.
     * `IDataMaskingProfileContext` implementations should be aware of this class and call the
     * context-aware methods directly.
     *
     * Abstract functionality includes:
     *
     * - `createContext` must be implemented.
     *
     * Overridden functionality includes:
     *
     * - The default version is defined by the instance.
     * - The instance identity is defined as `configuration-element-name [ "(" name ")" ]`
     *
     * Extended functionality includes:
     *
     * - Support for tracking aceepted and used custom context properties.
     *
     * Configuration:
     *
     * - @minimumVersion:  Optional. See `Versioned`.
     * - @maximumVersion:  Optional. See `Versioned`.
     * - @name:            Optional text identifier. Profiles are referenced primarily by domain,
     *                     but names are supported for improved trace output.
     * - @defaultVersion:  Optional version number to be used when a caller does not specify one.
     *                     Omitted, empty, or *0* falls back to the maximum version.
     * - @domain:          Required identifier for associated related profiles.
     * - legacyDomain:     Optional repesting element specifying alternate domain identifiers that
     *                     can be used to identify the profile. Enables domains to be updated
     *                     without breaking backward compatibility.
     * - legacyDomain/@id: Required, non-empty domain identifier.
     * - property:         Optional repeating element used to declare acceptance of custom context
     *                     properties. Refer to `CProfileEntity` assuming `@name` is required.
     */
    class CProfile : implements IDataMaskingProfile, implements IDataMaskingProfileInspector, public Versioned, public CInterface
    {
    public: // IInterface
        IMPLEMENT_IINTERFACE;
    public: // IDataMasker
        virtual const char* queryDomain() const override
        {
            return queryDefaultDomain();
        }
        virtual uint8_t queryVersion() const override
        {
            return queryDefaultVersion();
        }
        virtual uint8_t supportedOperations() const override
        {
            return supportedOperations(ensureContext());
        }
        virtual bool maskValue(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const override
        {
            return maskValue(ensureContext(), valueType, maskStyle, buffer, offset, length, conditionally);
        }
        virtual bool maskContent(const char* contentType, char* buffer, size_t offset, size_t length) const override
        {
            return maskContent(ensureContext(), contentType, buffer, offset, length);
        }
        virtual bool maskMarkupValue(const char* element, const char* attribute, char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback) const override
        {
            return maskMarkupValue(ensureContext(), element, attribute, buffer, offset, length, callback);
        }
    public: // IDataMaskingProfile
        virtual const IDataMaskingProfileInspector& inspector() const override
        {
            return *this;
        }
    public: // IDataMaskerInspector
        virtual bool isValid() const override
        {
            return Versioned::isValid();
        }
        virtual const char* queryDefaultDomain() const override
        {
            return defaultDomainId;
        }
        virtual bool acceptsDomain(const char* id) const override
        {
            if (isEmptyString(id))
                return false;
            return (domainIds.find(id) != domainIds.end());
        }
        virtual ITextIterator* getAcceptedDomains() const override
        {
            using SrcIterator = std::set<std::string>::const_iterator;
            using DomainIterator = TIteratorOfScalar<const char*, SrcIterator>;
            return new DomainIterator(domainIds.begin(), domainIds.end(), StdStringExtractor<SrcIterator>());
        }
        virtual uint8_t queryMinimumVersion() const override
        {
            return Versioned::queryMinimumVersion();
        }
        virtual uint8_t queryMaximumVersion() const override
        {
            return Versioned::queryMaximumVersion();
        }
        virtual uint8_t queryDefaultVersion() const override
        {
            return defaultVersion;
        }
        virtual IDataMaskingProfileValueType* queryValueType(const char* name) const override
        {
            return queryValueType(ensureContext(), name);
        }
        virtual bool hasValueType() const override
        {
            return hasValueType(ensureContext());
        }
        virtual size_t countOfValueTypes() const override
        {
            return countOfValueTypes(ensureContext());
        }
        virtual IDataMaskingProfileValueTypeIterator* getValueTypes() const override
        {
            return getValueTypes(ensureContext());
        }
        virtual bool acceptsProperty(const char* name) const override
        {
            return acceptsProperty(ensureContext(), name);
        }
        virtual ITextIterator* getAcceptedProperties() const override
        {
            return getAcceptedProperties(ensureContext());
        }
        virtual bool usesProperty(const char* name) const override
        {
            return usesProperty(ensureContext(), name);
        }
        virtual ITextIterator* getUsedProperties() const override
        {
            return getUsedProperties(ensureContext());
        }
        virtual bool hasRule(const char* contentType) const override
        {
            return hasRule(ensureContext(), contentType);
        }
        virtual size_t countOfRules(const char* contentType) const override
        {
            return countOfRules(ensureContext(), contentType);
        }
        virtual bool getMarkupValueInfo(const char* element, const char* attribute, const char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback, DataMaskingMarkupValueInfo& info) const override
        {
            return getMarkupValueInfo(ensureContext(), element, attribute, buffer, offset, length, callback, info);
        }
    public: // Versioned
        virtual ITracer& tracerRef() const override
        {
            return *tracer;
        }
        virtual ITracer& tracerRef(const IDataMaskingProfileContext* context) const override
        {
            if (!context || !context->queryTracer())
                return *tracer;
            return *context->queryTracer();
        }
        virtual const char* tracePrefix() const override
        {
            return path.c_str();
        }
        virtual bool configure(const IPTree& configuration) override
        {
            configureName(configuration);
            configureVersions(configuration);
            if (!configureDomains(configuration))
                valid = false;
            if (!configureAcceptedProperties(configuration))
                valid = false;
            return valid;
        }
    protected:
        PropertyCoverageMap                       properties;
        uint8_t                                   defaultVersion = 0;
    private:
        mutable Owned<IDataMaskingProfileContext> defaultCtx;
        mutable CriticalSection                   defaultCtxLock;
        Linked<ITracer>                           tracer;
        std::string                               name;
        std::set<std::string>                     domainIds;
        const char*                               defaultDomainId = nullptr; // always points into domainIds
        std::string                               path;
    public:
        CProfile(ITracer& _tracer)
            : Versioned(nullptr)
        {
            tracer.set(&_tracer);
        }
    protected:
        virtual bool configureName(const IPTree& configuration)
        {
            path += configuration.queryName();
            const char* tmp = configuration.queryProp("@name");
            if (!isEmptyString(tmp))
            {
                name = tmp;
                path += "(";
                path += name;
                path += ")";
            }
            return true;
        }
        virtual bool configureVersions(const IPTree& configuration)
        {
            Versioned::configure(configuration);
            defaultVersion = readVersion(configuration, "@defaultVersion", maximumVersion);
            return valid;
        }
        virtual bool configureDomains(const IPTree& configuration)
        {
            bool invalidated = false;
            std::pair<std::set<std::string>::iterator, bool> insertionResult;
            const char* tmp = configuration.queryProp("@domain");
            if (isEmptyString(tmp))
            {
                tracerRef().uerrlog("%s: missing required domain identifier", this->tracePrefix());
                invalidated = true;
            }
            else
            {
                insertionResult = domainIds.insert(tmp);
                defaultDomainId = insertionResult.first->c_str();
            }
            Owned<IPTreeIterator> legacyDomains(configuration.getElements("legacyDomain"));
            ForEach(*legacyDomains)
            {
                tmp = legacyDomains->query().queryProp("@id");
                if (isEmptyString(tmp))
                {
                    tracerRef().uerrlog("%s: missing legacyDomain/@id", this->tracePrefix());
                    invalidated = true;
                }
                else
                {
                    insertionResult = domainIds.insert(tmp);
                    if (false == insertionResult.second)
                        tracerRef().uwarnlog("%s: duplication legacyDomain/@id '%s'", this->tracePrefix(), tmp);
                }
            }
            if (invalidated && valid)
                valid = false;
            return !invalidated;
        }
        virtual bool configureAcceptedProperties(const IPTree& configuration)
        {
            using Property = CProfileEntity;
            Owned<IPTreeIterator> it(configuration.getElements("property"));
            ForEach(*it)
            {
                Property property(*this, true);
                if (property.configure(it->query()))
                {
                    properties[property.queryName()].accept(property);
                }
            }
            return true;
        }
        virtual IDataMaskingProfileContext& ensureContext() const
        {
            if (!defaultCtx)
            {
                CriticalBlock block(defaultCtxLock);
                if (!defaultCtx)
                {
                    defaultCtx.setown(createContext(0, tracer));
                }
            }
            return *defaultCtx;
        }
    public:
        virtual uint8_t supportedOperations(const IDataMaskingProfileContext& context) const
        {
            return 0;
        }
        virtual bool maskValue(const IDataMaskingProfileContext& context, const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const
        {
            return false;
        }
        virtual bool maskContent(const IDataMaskingProfileContext& context, const char* contentType, char* buffer, size_t offset, size_t length) const
        {
            return false;
        }
        virtual bool maskMarkupValue(const IDataMaskingProfileContext& context, const char* element, const char* attribute, char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback) const
        {
            return false;
        }
        virtual IDataMaskingProfileValueType* queryValueType(const IDataMaskingProfileContext& context, const char* name) const
        {
            return nullptr;
        }
        virtual bool hasValueType(const IDataMaskingProfileContext& context) const
        {
            return false;
        }
        virtual size_t countOfValueTypes(const IDataMaskingProfileContext& context) const
        {
            return 0;
        }
        virtual IDataMaskingProfileValueTypeIterator* getValueTypes(const IDataMaskingProfileContext& context) const
        {
            using Empty = std::list<Owned<IDataMaskingProfileValueType> >;
            using SrcIterator = Empty::const_iterator;
            using ValueTypeIterator = TIteratorOfShared<IDataMaskingProfileValueType, SrcIterator>;
            static const Empty empty;
            return new ValueTypeIterator(empty.begin(), empty.end(), extractShared<IDataMaskingProfileValueType, SrcIterator>);
        }
        virtual bool acceptsProperty(const IDataMaskingProfileContext& context, const char* name) const
        {
            if (isEmptyString(name))
                return false;
            typename PropertyCoverageMap::const_iterator it = properties.find(name);
            return (it != properties.end() ? it->second.accepted.test(context.queryVersion()) : false);
        }
        virtual ITextIterator* getAcceptedProperties(const IDataMaskingProfileContext& context) const
        {
            using SrcIterator = typename PropertyCoverageMap::const_iterator;
            struct Filter
            {
                uint8_t version = 0;
                bool operator () (SrcIterator it) const { return it->second.accepted.test(version); }
                Filter(uint8_t _version) : version(_version) {}
            };
            return new TIteratorOfScalar<const char*, SrcIterator>(properties.begin(), properties.end(), StdStringKeyExtractor<SrcIterator>(), Filter(context.queryVersion()));
        }
        virtual bool usesProperty(const IDataMaskingProfileContext& context, const char* name) const
        {
            if (isEmptyString(name))
                return false;
            typename PropertyCoverageMap::const_iterator it = properties.find(name);
            return (it != properties.end() ? it->second.used.test(context.queryVersion()) : false);
        }
        virtual ITextIterator* getUsedProperties(const IDataMaskingProfileContext& context) const
        {
            using SrcIterator = typename PropertyCoverageMap::const_iterator;
            struct Filter
            {
                uint8_t version = 0;
                bool operator () (SrcIterator it) const { return it->second.used.test(version); }
                Filter(uint8_t _version) : version(_version) {}
            };
            return new TIteratorOfScalar<const char*, SrcIterator>(properties.begin(), properties.end(), StdStringKeyExtractor<SrcIterator>(), Filter(context.queryVersion()));
        }
        virtual bool hasRule(const IDataMaskingProfileContext& context, const char* contentType) const
        {
            return false;
        }
        virtual size_t countOfRules(const IDataMaskingProfileContext& context, const char* contentType) const
        {
            return 0;
        }
        virtual bool getMarkupValueInfo(const IDataMaskingProfileContext& context, const char* element, const char* attribute, const char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback, DataMaskingMarkupValueInfo& info) const
        {
            return false;
        }
    };

    /**
     * @brief Implementation of `IDataMaskingProfileContext`.
     *
     * Use of a context is generally through the declared public interface. This is a minimalist
     * implementation of the interface, with no added functionality.
     *
     * This implementation is not thread safe. Two or more threads may share a context once it is
     * no longer subject to change. Each thread needing its own contextual values should own its
     * own copy of a context.
     */
    class CContext : implements IDataMaskingProfileContext, implements IDataMaskingProfileContextInspector, public CInterface
    {
    protected:
        using PropertyMap = std::map<std::string, std::string>;
    public: // IInterface
        IMPLEMENT_IINTERFACE;
    public: // IDataMasker
        virtual const char* queryDomain() const override
        {
            return queryDefaultDomain();
        }
        virtual uint8_t queryVersion() const override
        {
            return version;
        }
        virtual uint8_t supportedOperations() const override
        {
            return profile->supportedOperations();
        }
        virtual bool maskValue(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const override
        {
            return profile->maskValue(*this, valueType, maskStyle, buffer, offset, length, conditionally);
        }
        virtual bool maskContent(const char* contentType, char* buffer, size_t offset, size_t length) const override
        {
            return profile->maskContent(*this, contentType, buffer, offset, length);
        }
        virtual bool maskMarkupValue(const char* element, const char* attribute, char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback) const override
        {
            return profile->maskMarkupValue(*this, element, attribute, buffer, offset, length, callback);
        }
    public: // IDataMaskingProfileContext
        virtual ITracer* queryTracer() const override
        {
            return tracer;
        }
        virtual bool hasProperties() const override
        {
            return !properties.empty();
        }
        virtual bool hasProperty(const char* name) const override
        {
            if (isEmptyString(name))
                return false;
            PropertyMap::const_iterator it = find(name);
            return (it != properties.end());
        }
        virtual const char* queryProperty(const char* name) const override
        {
            if (isEmptyString(name))
                return nullptr;
            PropertyMap::const_iterator it = find(name);
            return (it != properties.end() ? it->second.c_str() : nullptr);
        }
        virtual bool setProperty(const char* name, const char* value) override
        {
            if (acceptsProperty(name) && value)
            {
                properties[name] = value;
                return true;
            }
            return false;
        }
        virtual bool removeProperty(const char* name) override
        {
            if (isEmptyString(name))
                return false;
            std::map<std::string, std::string>::iterator it = find(name);
            if (it != properties.end())
            {
                properties.erase(it);
                return true;
            }
            return false;
        }
        virtual CContext* clone() const override
        {
            return new CContext(*this);
        }
        virtual const IDataMaskingProfileContextInspector& inspector() const override
        {
            return *this;
        }
    public: // IDataMaskerInspector
        virtual bool isValid() const override
        {
            return profile->isValid();
        }
        virtual const char* queryDefaultDomain() const override
        {
            return profile->queryDefaultDomain();
        }
        virtual bool acceptsDomain(const char* id) const override
        {
            return profile->acceptsDomain(id);
        }
        virtual ITextIterator* getAcceptedDomains() const override
        {
            return profile->getAcceptedDomains();
        }
        virtual uint8_t queryMinimumVersion() const override
        {
            return profile->queryMinimumVersion();
        }
        virtual uint8_t queryMaximumVersion() const override
        {
            return profile->queryMaximumVersion();
        }
        virtual uint8_t queryDefaultVersion() const override
        {
            return profile->queryDefaultVersion();
        }
        virtual IDataMaskingProfileValueType* queryValueType(const char* name) const override
        {
            return profile->queryValueType(*this, name);
        }
        virtual bool hasValueType() const override
        {
            return profile->hasValueType(*this);
        }
        virtual size_t countOfValueTypes() const override
        {
            return profile->countOfValueTypes(*this);
        }
        virtual IDataMaskingProfileValueTypeIterator* getValueTypes() const override
        {
            return profile->getValueTypes(*this);
        }
        virtual bool acceptsProperty(const char* name) const override
        {
            return profile->acceptsProperty(*this, name);
        }
        virtual ITextIterator* getAcceptedProperties() const override
        {
            return profile->getAcceptedProperties(*this);
        }
        virtual bool usesProperty(const char* name) const override
        {
            return profile->usesProperty(*this, name);
        }
        virtual ITextIterator* getUsedProperties() const override
        {
            return profile->getUsedProperties(*this);
        }
        virtual bool hasRule(const char* contentType) const override
        {
            return profile->hasRule(*this, contentType);
        }
        virtual size_t countOfRules(const char* contentType) const override
        {
            return profile->countOfRules(*this, contentType);
        }
        virtual bool getMarkupValueInfo(const char* element, const char* attribute, const char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback, DataMaskingMarkupValueInfo& info) const override
        {
            return profile->getMarkupValueInfo(*this, element, attribute, buffer, offset, length, callback, info);
        }
    public: // IDataMaskingProfileContextInspector
        virtual IDataMaskingContextPropertyIterator* getProperties() const override
        {
            struct PropertyExtractor : implements IDataMaskingContextProperty
            {
                PropertyMap::const_iterator it;
                const char* name = nullptr;
                const char* value = nullptr;
                virtual const char* queryName() const override { return name; }
                virtual const char* queryValue() const override { return value; }
                IDataMaskingContextProperty& operator () (PropertyMap::const_iterator _it)
                {
                    if (it != _it)
                    {
                        it = _it;
                        name = it->first.c_str();
                        value = it->second.c_str();
                    }
                    return *this;
                }
                PropertyExtractor(PropertyMap::const_iterator _it)
                    : it(_it)
                {
                }
            };
            return new TIteratorOfScalar<IDataMaskingContextProperty, PropertyMap::const_iterator>(properties.begin(), properties.end(), PropertyExtractor(properties.end()));
        }
        virtual const IDataMaskingProfile& queryProfile() const override
        {
            return *profile;
        }
    protected:
        Linked<const CProfile> profile;
        uint8_t                version = 0;
        Linked<ITracer>        tracer;
        PropertyMap            properties;
    public:
        CContext(const CProfile& _profile, uint8_t _version, ITracer* _tracer)
            : version(_version)
            , tracer(_tracer)
        {
            profile.set(&_profile);
        }
        CContext(const CContext& original)
            : profile(original.profile.get())
            , version(original.version)
            , tracer(original.tracer.get())
            , properties(original.properties)
        {
        }
    protected:
        std::map<std::string, std::string>::const_iterator find(const char* name) const
        {
            if (isEmptyString(name))
                return properties.end();
            return properties.find(name);
        }
        std::map<std::string, std::string>::iterator find(const char* name)
        {
            if (isEmptyString(name))
                return properties.end();
            return properties.find(name);
        }
    };

    /**
     * @brief Templated extension of `CProfile` adding support for maskValue and rule ownership
     *        (to be exploited by subclasses for `maskContent` support).
     *
     * Overridden functionality includes:
     *
     * - The default version is defined by the instance.
     * - The instance identity is defined as `configuration-element-name [ "(" name ")" ]`
     *
     * Extended functionality includes:
     *
     * - Creation of value types of type `valuetype_t`
     * - Defined storage for rules of type `rule_t` created by the value types.
     * - Value obfuscation conditional on defined value types and context.
     *
     * Configuration:
     *
     * - @minimumVersion:  Optional. See `Versioned`.
     * - @maximumVersion:  Optional. See `Versioned`.
     * - @name:            Optional. See `CProfile`.
     * - @defaultVersion:  Optional. See `CProfile`.
     * - @domain:          Optional. See `CProfile`.
     * - legacyDomain:     Optional. See `CProfile`.
     * - legacyDomain/@id: Required. See `CProfile`.
     * - property:         Optional. See `CProfile`.
     * - valueType:        Required. See the definition of `valuetype_t` for more information.
     *
     * @tparam valuetype_t
     * @tparam rule_t
     * @tparam context_t
     */
    template <typename valuetype_t, typename rule_t, typename context_t>
    class TProfile : public CProfile
    {
    public:
        using Self = TProfile<valuetype_t, rule_t, context_t>;
        using ValueTypeMap = std::multimap<std::string, Owned<valuetype_t> >; // value type name -> value type
    public: // IDataMasker
        virtual uint8_t supportedOperations() const override
        {
            return (DataMasking_MaskValue);
        }
        virtual bool maskValue(const IDataMaskingProfileContext& context, const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const override
        {
            valuetype_t* vt = queryValueType(context, valueType);
            if (!vt)
            {
                if (!conditionally && valueType && !streq(valueType, DataMasking_UnconditionalValueTypeName))
                    vt = queryValueType(context, DataMasking_UnconditionalValueTypeName);
                if (!vt)
                    return false;
            }
            return vt->applyMask(&context, maskStyle, buffer, offset, length);
        }
    public:
        virtual context_t* createContext(uint8_t version, ITracer* tracer) const override
        {
            if (0 == version)
                version = defaultVersion;
            if (this->includes(version, version))
                return new context_t(*this, version, tracer);
            return nullptr;
        }
        virtual valuetype_t* queryValueType(const IDataMaskingProfileContext& context, const char* name) const override
        {
            if (isEmptyString(name))
                return nullptr;
            auto range = valueTypes.equal_range(name);
            if (valueTypes.end() == range.first)
                return nullptr;
            for (auto it = range.first; it != range.second; ++it)
            {
                if (it->second->matches(&context))
                    return it->second;
            }
            return nullptr;
        }
        virtual bool hasValueType(const IDataMaskingProfileContext& context) const override
        {
            for (const typename ValueTypeMap::value_type& entry : valueTypes)
            {
                if (entry.second->matches(&context))
                    return true;
            }
            return false;
        }
        virtual size_t countOfValueTypes(const IDataMaskingProfileContext& context) const override
        {
            size_t count = 0;
            for (const typename ValueTypeMap::value_type& entry : valueTypes)
            {
                if (entry.second->matches(&context))
                    count++;
            }
            return count;
        }
        virtual IDataMaskingProfileValueTypeIterator* getValueTypes(const IDataMaskingProfileContext& context) const override
        {
            using SrcIterator = typename ValueTypeMap::const_iterator;
            using ValueTypeIterator = TIteratorOfShared<IDataMaskingProfileValueType, SrcIterator>;
            struct Filter
            {
                const IDataMaskingProfileContext* context = nullptr;
                bool operator () (SrcIterator it) const { return (it->second->matches(context)); }
                Filter(const IDataMaskingProfileContext& _context) : context(&_context) {}
            };
            return new ValueTypeIterator(valueTypes.begin(), valueTypes.end(), extractMappedShared<IDataMaskingProfileValueType, SrcIterator>, Filter(context));
        }

        virtual bool hasRule(const IDataMaskingProfileContext& context, const char* contentType) const override
        {
            if (isEmptyString(contentType))
            {
                for (const Owned<rule_t>& rule : rules)
                {
                    if (checkRule(context, rule))
                        return true;
                }
            }
            else
            {
                for (const Owned<rule_t>& rule : rules)
                {
                    if (checkRule(context, rule, contentType))
                        return true;
                }
            }
            return false;
        }
        virtual size_t countOfRules(const IDataMaskingProfileContext& context, const char* contentType) const override
        {
            size_t count = 0;
            if (isEmptyString(contentType))
            {
                for (const Owned<rule_t>& rule : rules)
                {
                    if (checkRule(context, rule))
                        count++;
                }
            }
            else
            {
                for (const Owned<rule_t>& rule : rules)
                {
                    if (checkRule(context, rule, contentType))
                        count++;
                }
            }
            return count;
        }
        virtual bool configure(const IPTree& configuration) override
        {
            CProfile::configure(configuration);
            if (!configureValueTypes(configuration))
                valid = false;
            return valid;
        }
    protected:
        ValueTypeMap           valueTypes;
        RuleList<rule_t>       rules;
    public:
        using CProfile::CProfile;
    protected:
        virtual bool configureValueTypes(const IPTree& configuration)
        {
            bool invalidated = false;
            CoverageMap coverages;
            Owned<IPTreeIterator> it(configuration.getElements("valueType"));
            ForEach(*it)
            {
                Owned<valuetype_t> valueType(createValueType(it->query()));
                if (!valueType)
                    continue;
                const char* key = valueType->queryName();
                if (isEmptyString(key))
                {
                    invalidated = true;
                    continue;
                }
                DataMaskingVersionCoverage& coverage = coverages[key];
                if (coverage.any())
                {
                    DataMaskingVersionCoverage tmp;
                    valueType->update(tmp);
                    tmp &= coverage;
                    if (tmp.any())
                    {
                        tracerRef().uerrlog("%s: invalid version coverage for value type '%s'; version overlap", this->tracePrefix(), key);
                        invalidated = true;
                    }
                }
                valueType->update(coverage);
                valueTypes.emplace(key, valueType.getClear());
            }
            if (invalidated && valid)
                valid = false;
            return !invalidated;
        }
        virtual valuetype_t* createValueType(const IPTree& configuration)
        {
            Owned<valuetype_t> valueType(createValueType());
            return (valueType && valueType->configure(configuration, properties, rules) ? valueType.getClear() : nullptr);
        }
        virtual valuetype_t* createValueType()
        {
            return new valuetype_t(*this);
        }
    private:
        inline bool checkRule(const IDataMaskingProfileContext& context, const rule_t* rule, const char* contentType) const
        {
            const char* ruleContentType = rule->queryContentType();
            return (rule->matches(&context) && (isEmptyString(ruleContentType) || streq(contentType, ruleContentType)));
        }
        inline bool checkRule(const IDataMaskingProfileContext& context, const rule_t* rule) const
        {
            return rule->matches(&context);
        }
    };

    /**
     * @brief Self contained implementation of `IDataMaskingProfileIterator`.
     *
     * Configuration:
     *
     * - profile: Optional repeating element used to configure a profile. If omitted, the entire
     *            configuration is treated as a profile configuration. See definition of
     *            `profile_t` for more information.
     *
     * @tparam profile_t
     */
    template <typename profile_t>
    class TPlugin : public CInterfaceOf<IDataMaskingProfileIterator>
    {
    protected:
        using ProfileList = std::list<Owned<profile_t> >;
    public:
        virtual bool first() override
        {
            cur = profiles.begin();
            return isValid();
        }
        virtual bool next() override
        {
            if (isValid())
            {
                ++cur;
                return isValid();
            }
            return false;
        }
        virtual bool isValid() override
        {
            return (cur != profiles.end());
        }
        virtual IDataMaskingProfile& query() override
        {
            if (!isValid())
                throw makeStringException(-1, "invalid data masking plugin query");
            return *(*cur);
        }
    protected:
        Linked<ITracer>                      tracer;
        ProfileList                          profiles;
        typename ProfileList::const_iterator cur;
    public:
        TPlugin(ITracer& _tracer)
            : tracer(&_tracer)
        {
            cur = profiles.end();
        }
        virtual bool configure(const IPTree& configuration)
        {
            Owned<IPTreeIterator> it(configuration.getElements("profile"));
            ForEach(*it)
            {
                configureProfile(it->query());
            }
            if (profiles.empty())
            {
                configureProfile(configuration);
            }
            cur = profiles.end();
            return !profiles.empty();
        }
    protected:
        virtual bool configureProfile(const IPTree& configuration)
        {
            Owned<profile_t> profile(createProfile(configuration));
            if (profile)
            {
                profiles.emplace_back(profile.getLink());
                return true;
            }
            return false;
        }
        virtual profile_t* createProfile(const IPTree& configuration)
        {
            Owned<profile_t> profile(createProfile());
            if (!profile)
            {
                tracer->ierrlog("%s: profile creation failed", configuration.queryName());
            }
            else if (!profile->configure(configuration))
            {
                tracer->ierrlog("%s: profile configuration failed", profile->tracePrefix());
                profile.clear();
            }
            return profile.getClear();
        }
        virtual profile_t* createProfile()
        {
            return new profile_t(*tracer);
        }
    };

    /**
     * @brief A `CMaskStyle` extension that masks part of a value.
     *
     * There are multiple use cases for applying a mask to only part of the value. Credit card
     * numbers, telephone numbers, and US Social Security Numbers, are frequently presented with
     * partially masked, revealing enough information to confirm the proper value without exposing
     * the complete value. Consider a credit card receipt showing the card's last four digits.
     *
     * The style is expressed as:
     *
     *     style ::= [ action ] [ location ] count [ characters ]
     *     action = ( "" | "mask" | "keep" )
     *     location = ( "" | "first" | "last" )
     *     count ::= digits
     *     characters ::= ( "" | "numbers" | "letters" | "alphanumeric" | "all" )
     *
     * The default action is `mask`, resulting in the identified buffer substring being masked. A
     * value of `keep` results in the remaining substring being masked.
     *
     * The default location is `last`, identifying the rightmost characters. A value of `first`
     * identifies the leftmost characters.
     *
     * The default characters selector is `numbers`, indicating that `count` refers only to numeric
     * characters; numeric values frequently represented with embedded delimiters can exclude these
     * delimiters when defining the mask substring. Other values include `letters` for alphabetic
     * characters, `alphanumeric` for letters or numbers, and `all` meaning every character is
     * counted.
     *
     * Configuration:
     *
     * - @minimumVersion:  Optional. See `Versioned`.
     * - @maximumVersion:  Optional. See `Versioned`.
     * - @name:             Required. See `CProfileEntity`
     * - @pattern:          Optional character sequence used to mask text. Affected characters are
     *                      replaced by asterisks if omitted or empty.
     * - @overrideDefault: Optional Boolean flag indicating whether the instance is a replacement
     *                      for the default mask style of the containing value type. False if
     *                      omitted or empty.
     * - @action:           Optional choice of *mask* or *keep*. Omitted or empty defaults to
     *                      *mask*.
     * - @location:         Optional choice of *last* or *first*. Omitted or empty defaults to
     *                      *last*.
     * - @count:            Positive integer. Required if any of `@action`, `@location`, or
     *                      `@characters` are included (even if empty). Omission when not required
     *                      causes all characters to be masked.
     * - @characters:       Optional choice of *numbers*, *letters*, *alphanumeric*, or *all*.
     *                      Omitted  or empty defaults to *all*.
     */
    class CPartialMaskStyle : public CMaskStyle
    {
    public:
        virtual bool configure(const IPTree& configuration, PropertyCoverageMap& propertyCoverages) override
        {
            CMaskStyle::configure(configuration, propertyCoverages);
            if (!configureBehavior(configuration))
                valid = false;
            return valid;
        }
        virtual bool applyMask(const IDataMaskingProfileContext* context, char* buffer, size_t offset, size_t length) const override
        {
            if (isEmptyString(buffer) || (0 == length))
                return false;
            if (0 == characterCount)
                return CMaskStyle::applyMask(context, buffer, offset, length);
            if (std::numeric_limits<size_t>::max() == length)
                length = strlen(buffer);

            size_t actualOffset = offset;
            size_t actualLength = 0;
            if (characterTester)
            {
                size_t matchCount = 0;
                if (First == location)
                {
                    while (actualLength < length && matchCount < characterCount)
                    {
                        if (characterTester(buffer[actualLength]))
                            matchCount++;
                        actualLength++;
                    }
                }
                else
                {
                    actualOffset += length;
                    while (actualOffset > offset && matchCount < characterCount)
                    {
                        --actualOffset;
                        if (characterTester(buffer[actualOffset]))
                            matchCount++;
                        actualLength++;
                    }
                }
            }
            else
            {
                if (First == location)
                {
                    actualLength = std::min(length, size_t(characterCount));
                }
                else
                {
                    if (length <= characterCount)
                    {
                        actualLength = length;
                    }
                    else
                    {
                        actualOffset += (length - characterCount);
                        actualLength = characterCount;
                    }
                }
            }
            if (Mask == action)
                return maskSubstring(buffer + actualOffset, actualLength);
            if (actualOffset > offset)
                return maskSubstring(buffer + offset, actualOffset - offset);
            if (actualLength < length)
                return maskSubstring(buffer + offset + actualLength, length - actualLength);
            return false;
        }
    protected:
        using Tester = int (*)(int);
        enum Action { Mask, Retain };
        enum Location { Last, First };
        Action action = Mask;
        Location location = Last;
        Tester characterTester = nullptr;
        long characterCount = 0;
    public:
        using CMaskStyle::CMaskStyle;
        virtual bool configureBehavior(const IPTree& configuration)
        {
            bool invalidated = false;
            bool requireCount = false;
            const char* tmp = configuration.queryProp("@action");
            if (!requireCount && tmp)
                requireCount = true;
            if (!isEmptyString(tmp))
            {
                if (streq(tmp, "mask"))
                    action = Mask;
                else if (streq(tmp, "keep"))
                    action = Retain;
                else
                {
                    tracerRef().uerrlog("%s: invalid action value '%s'", tracePrefix(), tmp);
                    invalidated = true;
                }
            }
            tmp = configuration.queryProp("@location");
            if (!requireCount && tmp)
                requireCount = true;
            if (!isEmptyString(tmp))
            {
                if (streq(tmp, "last"))
                    location = Last;
                else if (streq(tmp, "first"))
                    location = First;
                else
                {
                    tracerRef().uerrlog("%s: invalid character location value '%s'", tracePrefix(), tmp);
                    invalidated = true;
                }
            }
            tmp = configuration.queryProp("@characters");
            if (!requireCount && tmp)
                requireCount = true;
            if (!isEmptyString(tmp))
            {
                if (streq(tmp, "numbers"))
                    characterTester = &isdigit;
                else if (streq(tmp, "letters"))
                    characterTester = &isalpha;
                else if (streq(tmp, "alphanumeric"))
                    characterTester = &isalnum;
                else if (streq(tmp, "all"))
                    characterTester = nullptr;
                else
                {
                    tracerRef().uerrlog("%s: invalid character selector value '%s'", tracePrefix(), tmp);
                    invalidated = true;
                }
            }
            tmp = configuration.queryProp("@count");
            if (!tmp)
            {
                if (requireCount)
                {
                    tracerRef().uerrlog("%s: missing required count value", tracePrefix());
                    invalidated = true;
                }
            }
            else if (!*tmp)
            {
                tracerRef().uerrlog("%s: invalid empty count value", tracePrefix());
                invalidated = true;
            }
            else
            {
                char* endptr = nullptr;
                long count = strtol(tmp, &endptr, 0);
                while (isspace(*endptr)) endptr++;
                if (*endptr)
                {
                    tracerRef().uerrlog("%s: invalid character count content '%s'", tracePrefix(), tmp);
                    invalidated = true;
                }
                else if (count > 0)
                    characterCount = count;
                else // if (count <= 0)
                {
                    tracerRef().uerrlog("%s: invalid character count value %ld", tracePrefix(), count);
                    invalidated = true;
                }
            }
            return !invalidated;
        }
    protected:
        /**
         * @brief Applies the defined mask pattern to a range in the buffer.
         *
         * A subclass might override this method to replace the masking of all characters in the
         * range to only masking specific characters. For example, masking part of a phone number
         * containing delimiters might only mask digits and leave the delimiters unchanged.
         *
         * @param buffer
         * @param length
         * @return true
         * @return false
         */
        virtual bool maskSubstring(char* buffer, size_t length) const
        {
            if (pattern.size() == 1)
            {
                for (size_t bufferIdx = 0; bufferIdx < length; bufferIdx++)
                {
                    buffer[bufferIdx] = pattern.at(0);
                }
            }
            else
            {
                for (size_t bufferIdx = 0, patternIdx = 0; bufferIdx < length; bufferIdx++)
                {
                    if (pattern.size() == patternIdx)
                        patternIdx = 0;
                    buffer[bufferIdx] = pattern.at(patternIdx++);
                }
            }
            return true;
        }
    };

    /**
     * @brief An extension of CRule that scans the input buffer for a start and an end token, and
     *        masks the content in between.
     *
     * Intended for use with `TSerialProfile`, or similar class, masking is applied using this
     * general algorithm:
     *
     *     while start token is found in input
     *         if end token is found after start token
     *             apply defining value type's default mask to characters between tokens
     *         end if
     *     end while
     *
     * Configuration:
     *
     * - @startToken: Required character sequence which, if followed by a corresponding end
     *                sequence, defines the beginning of a high risk value.
     * - @endToken:   Optional character sequence which, if preceded by a corresponding stsart
     *                sequence, defines the end of a high risk value. Omitted or empty defaults
     *                to a line feed.
     * - @matchCase:  Optional Boolean flag indicating whether or not token comparisons are case
     *                sensitive. Omission or empty defaults to *false*, meaning comparisons are
     *                case insensitive.
     */
    class CSerialTokenRule : public CRule
    {
    protected:
        class Terminator
        {
            char* endptr;
            char  cached;
        public:
            Terminator(char* _endptr)
                : endptr(_endptr)
                , cached(*_endptr)
            {
                *endptr = '\0';
            }
            ~Terminator()
            {
                *endptr = cached;
            }
        };
    public:
        virtual bool configure(const IPTree& configuration, PropertyCoverageMap& propertyCoverages) override
        {
            CRule::configure(configuration, propertyCoverages);
            if (!configureTokens(configuration))
                valid = false;
            if (!configureCase(configuration))
                valid = false;
            return valid;
        }
    protected:
        typedef char* (*Finder)(const char*, const char*);
        //using Finder = std::function<char*(const char*, const char*)>;
        std::string startToken;
        std::string endToken;
        Finder      finder = nullptr;
    public:
        using CRule::CRule;
        virtual bool applyRule(const IDataMaskingProfileContext& context, char* buffer, size_t length) const
        {
            if (isEmptyString(buffer) || 0 == length)
                return false;
            if (std::numeric_limits<size_t>::max() == length || '\0' == buffer[length])
            {
                return applyRule(context, buffer);
            }
            else
            {
                Terminator terminator(&buffer[length]);
                return applyRule(context, buffer);
            }
        }
    protected:
        virtual bool configureTokens(const IPTree& configuration)
        {
            bool invalidated = false;
            const char* tmp = configuration.queryProp("@startToken");
            if (isEmptyString(tmp))
            {
                tracerRef().uerrlog("%s: missing required start token", tracePrefix());
                invalidated = true;
            }
            else
                startToken = tmp;
            tmp = configuration.queryProp("@endToken");
            if (isEmptyString(tmp))
            {
                tracerRef().log(MCuserInfo, "%s: missing end token; assuming EOLN", tracePrefix());
                endToken = "\n";
            }
            else
                endToken = tmp;
            return !invalidated;
        }
        virtual bool configureCase(const IPTree& configuration)
        {
            if (configuration.getPropBool("@matchCase", false))
                finder = strstr_sensitive;
            else
                finder = strstr_insensitive;
            return true;
        }
        virtual bool applyRule(const IDataMaskingProfileContext& context, char* buffer) const
        {
            bool applied = false;
            char* start = finder(buffer, startToken.c_str());
            char* end;
            while (start)
            {
                start += startToken.length();
                end = finder(start, endToken.c_str());
                if (end)
                {
                    if (valueType.applyMask(&context, nullptr, start, 0, end - start))
                        applied = true;
                    start = finder(end + endToken.length(), startToken.c_str());
                }
                else
                    start = nullptr;
            }
            return applied;
        }
    private:
        inline static char* strstr_sensitive(const char* haystack, const char* needle)
        {
            return const_cast<char*>(strstr(haystack, needle));
        }
        inline static char* strstr_insensitive(const char* haystack, const char* needle)
        {
            if (!*needle)
                return const_cast<char*>(haystack);

            size_t idx;
            for (; *haystack; haystack++) {
                if (toupper((unsigned char)*haystack) == toupper((unsigned char)*needle)) {
                    for (idx = 1;; idx++) {
                        if (!needle[idx])
                            return const_cast<char*>(haystack);
                        if (toupper((unsigned char)haystack[idx]) != toupper((unsigned char)needle[idx]))
                            break;
                    }
                }
            }
            return nullptr;
        }
    };

    /**
     * @brief Concrete derivative of TProfile that identifies all rules matching request
     *        parameters and sequentially applies each rule to the input buffer.
     *
     * Extended functionality includes:
     *
     * - *maskContent* is implemented
     *
     * `rule_t` must implement:
     *      `bool applyRule(const IDataMaskingProfileContext& context,
     *                      char* buffer,
     *                      size_t length) const`
     * @tparam valuetype_t
     * @tparam rule_t
     * @tparam context_t
     */
    template <typename valuetype_t, typename rule_t, typename context_t>
    class TSerialProfile : public TProfile<valuetype_t, rule_t, context_t>
    {
    public:
        virtual uint8_t supportedOperations() const override
        {
            return (Base::supportedOperations() | DataMasking_MaskContent);
        }
    public:
        using Base = TProfile<valuetype_t, rule_t, context_t>;
        using Base::Base;
        using Base::tracerRef;
        using Base::tracePrefix;
        virtual bool maskContent(const IDataMaskingProfileContext& context, const char* contentType, char* buffer, size_t offset, size_t length) const override
        {
            if (!buffer)
                return false;
            buffer += offset;
            if (isEmptyString(buffer) || 0 == length)
                return false;

            bool masked = false;
            bool checkContentType = !isEmptyString(contentType);
            for (const Owned<rule_t>& r : this->rules)
            {
                if (!r)
                {
                    tracerRef(&context).ierrlog("%s: unexpected NULL rule", tracePrefix());
                    continue;
                }
                if (!r->matches(&context))
                    continue;
                if (checkContentType && !isEmptyString(r->queryContentType()) && !streq(r->queryContentType(), contentType))
                    continue;
                if (r->applyRule(context, buffer, length))
                    masked = true;
            }
            return masked;
        }
    };

    /**
     * @brief Abstract derivative of TProfile that identifies all rules matching mask request
     *        parameters and passes them to an abstract method, to be applied concurrently.
     *
     * This is an illustration of how parallel rule processing may be different from sequential
     * rule processing. A profile implementing parallel rule processing may extend or replace
     * this.
     *
     * @tparam valuetype_t
     * @tparam rule_t
     * @tparam context_t
     */
    template <typename valuetype_t, typename rule_t, typename context_t>
    class TParallelProfile : public TProfile<valuetype_t, rule_t, context_t>
    {
    public:
        virtual uint8_t supportedOperations() const override
        {
            return (Base::supportedIperations() | DataMasking_MaskContent);
        }
    protected:
        using Matches = std::list<const rule_t*>;
    public:
        using Base = TProfile<valuetype_t, rule_t, context_t>;
        using Base::Base;
        using Base::tracerRef;
        using Base::tracePrefix;
        virtual bool maskContent(const IDataMaskingProfileContext& context, const char* contentType, char* buffer, size_t offset, size_t length) const override
        {
            if (!buffer)
                return false;
            buffer += offset;
            if (isEmptyString(buffer) || 0 == length)
                return false;

            Matches* cache = getCache(contentType, context);
            if (cache && !cache->empty())
                return maskContent(*cache, buffer, length, context);

            std::list<const rule_t*> matchedRules;
            bool checkContentType = !isEmptyString(contentType);
            for (const Owned<rule_t>& r : this->rules)
            {
                if (!r)
                {
                    tracerRef(&context).ierrlog("%s: unexpected NULL rule", tracePrefix());
                    continue;
                }
                if (!r->matches(&context))
                    continue;
                if (checkContentType && (isEmptyString(r->queryContentType()) || !streq(r->queryContentType(), contentType)))
                    continue;
                matchedRules.push_back(r.get());
            }
            if (matchedRules.empty())
                return false;
            updateCache(contentType, context, matchedRules);
            return maskContent(matchedRules, buffer, length, context);
        }
    protected:
        virtual Matches* getCache(const IDataMaskingProfileContext& context, const char* contentType) const
        {
            return nullptr;
        }
        virtual void updateCache(const IDataMaskingProfileContext& context, const char* contentType, Matches& matches)
        {
        }
        virtual bool maskContent(const IDataMaskingProfileContext& context, const std::list<const rule_t*>& matchedRules, char* buffer, size_t length) const = 0;
    };

} // namespace DataMasking
