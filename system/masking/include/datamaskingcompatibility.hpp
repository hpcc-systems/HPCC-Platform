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

#include "jexcept.hpp"
#include "datamasking.h"
#include "tracer.hpp"

namespace DataMasking
{
    /**
     * @brief Utility class to determine if an engine supports a requested domain version and, if
     *        so, whether that domain version includes support expected by a caller.
     *
     * The class supports two approaches to testing - runtime and load-time. With runtime testing,
     * the caller is responsible for setting up the environment and executing individual tests as
     * needed. With load-time testing, the class accepts a structured configuration that describes
     * one or more contexts as well as a suite of tests to be run against each context.
     *
     * Runtime:
     *
     * 1. Associate an instance with an engine and optional tracer. If no tracer is given, a
     *    default modular tracer will be used.
     * 2. Select a domain version, using either a combination of identifier and version, a profile
     *    and version, or an existing context using `checkAndSetUsing`. If successful, a profile
     *    context will be available for internal use.
     * 3. Optionally add custom properties to the internal context with `checkUsingCustomProperty`.
     * 4. Perform requisite checks. Each check may specify a *presence* value indicating the
     *    expected outcome of the evaluation:
     *
     *   - Required ("r"): It is an error if the check is not satisfied. Default if omitted.
     *   - Optional ("o"): Record a warning if the check is not satisfied, without failing.
     *   - Prohibited ("p"): It is an error if the check is satisfied.
     *
     * Load-time:
     *
     * 1. Associate an instance with an engine and optional tracer. If no tracer
     *    is given, a default modular tracer will be used.
     * 2. Call `checkCompatibility` to run all configured tests. See the readme file description
     *    *Load-time Compatibility* for information on how to perform these tests.
     *
     * Available checks include:
     *
     * - Support for `maskValue`, `maskContent`, and `maskMarkupValue` operations.
     * - Custom context property acceptance.
     * - Custom context property usage.
     * - Selected value types.
     * - Mask styles for selected value types.
     * - Set memberships of selected value types.
     * - Presence of rules by content type.
     *
     * Tips and tricks:
     *
     * - When combining runtime and load-time testing, `checkAndSetUsing` must be called after
     *   a load-time test and before resuming runtime tests.
     * - Use custom properties. Checks related to value types and rules are constrained to the
     *   corresponding selected sets. If the sets that will be selected are unknown at the time
     *   a check is performed, consider selecting *any* set (using the reserved "\*" name)
     *   and check if the requirement can be satisfied.
     * - Test for unconditional value masking by checking for a value type with reserved name "\*".
     */
    class CCompatibilityChecker
    {
    public:
        enum Presence
        {
            Unknown = -1,
            Required,
            Optional,
            Prohibited,
        };
    private:
        Linked<const IDataMaskingEngine> engine;
        Owned<ITracer> tracer;
        Owned<IDataMaskingProfileContext> context;
    public:
        CCompatibilityChecker(const IDataMaskingEngine& _engine, ITracer* _tracer)
        {
            engine.set(&_engine);
            if (_tracer)
                tracer.set(_tracer);
            else
                tracer.setown(new CModularTracer());
        }
        bool checkAndSetUsing(const char* domainId, uint8_t version)
        {
            context.clear();
            if (!engine)
            {
                tracer->ierrlog("%s:%hhu: missing engine to select snapshot", domainId, version);
                return false;
            }
            IDataMaskingProfile* profile = engine->queryProfile(domainId, version);
            if (!profile)
            {
                if (version && engine->queryProfile(domainId, 0))
                    tracer->uerrlog("%s:%hhu: domain does not support version", domainId, version);
                else
                    tracer->uerrlog("%s:%hhu: unrecognized domain", domainId, version);
                return false;
            }
            return checkAndSetUsing(*profile, version);
        }
        inline bool checkAndSetUsing(const IDataMaskingProfile& _profile)
        {
            return checkAndSetUsing(_profile, 0);
        }
        bool checkAndSetUsing(const IDataMaskingProfile& _profile, uint8_t version)
        {
            Owned<IDataMaskingProfileContext> _context(_profile.createContext(version, tracer));
            if (!_context)
            {
                tracer->uerrlog("%s:%hhu: unexpected version out of range %hhu..%hhu", _profile.inspector().queryDefaultDomain(), version, _profile.inspector().queryMinimumVersion(), _profile.inspector().queryMaximumVersion());
                return false;
            }
            return checkAndSetUsing(_context.getClear());
        }
        bool checkAndSetUsing(IDataMaskingProfileContext* _context)
        {
            context.setown(_context);
            return true;
        }
        virtual bool checkCompatibility(const IPTree& requirements)
        {
            if (streq(requirements.queryName(), "compatibility"))
                return checkSnapshotCompatibility(requirements);
            bool compatible = true;
            Owned<IPTreeIterator> it(requirements.getElements("//compatibility"));
            ForEach(*it)
            {
                if (!checkSnapshotCompatibility(it->query()))
                    compatible = false;
            }
            return compatible;
        }
        bool checkSnapshotCompatibility(const IPTree& requirements)
        {
            bool failed = false;
            const IPTree* ctx = requirements.queryBranch("context");
            const char* domainId = nullptr;
            uint8_t version = 0;
            if (ctx)
            {
                int tmpVersion = ctx->getPropInt("@version");
                domainId = ctx->queryProp("@domain");
                if (0 <= tmpVersion && tmpVersion <= std::numeric_limits<uint8_t>::max())
                    version = uint8_t(tmpVersion);
                else
                {
                    tracer->uerrlog("invalid data masking domain version number %d", tmpVersion);
                    return false;
                }
            }
            if (!checkAndSetUsing(domainId, version))
                return false;
            if (ctx)
            {
                Owned<IPTreeIterator> it(ctx->getElements("property"));
                ForEach(*it)
                {
                    const IPTree& prop = it->query();
                    const char*   name = prop.queryProp("@name");
                    const char*   value = prop.queryProp("@value");
                    if (!checkUsingCustomProperty(name, value))
                        failed = true;
                }
                if (failed)
                    return false;
            }
            Owned<IPTreeIterator> it;
            it.setown(requirements.getElements("operation"));
            ForEach(*it)
            {
                const IPTree& node = it->query();
                const char* name = node.queryProp("@name");
                const char* presence = node.queryProp("@presence");
                if (!checkSupportsOperation(name, presence))
                    failed = true;
            }
            it.setown(requirements.getElements("accepts"));
            ForEach(*it)
            {
                const IPTree& node = it->query();
                const char* name = node.queryProp("@name");
                const char* presence = node.queryProp("@presence");
                if (!checkAcceptsProperty(name, presence))
                    failed = true;
            }
            it.setown(requirements.getElements("uses"));
            ForEach(*it)
            {
                const IPTree& node = it->query();
                const char* name = node.queryProp("@name");
                const char* presence = node.queryProp("@presence");
                if (!checkUsesProperty(name, presence))
                    failed = true;
            }
            it.setown(requirements.getElements("valueType"));
            ForEach(*it)
            {
                const IPTree& node = it->query();
                const char* vtName = node.queryProp("@name");
                const char* vtPresence = node.queryProp("@presence");
                if (!checkHasSelectedValueType(vtName, vtPresence))
                {
                    failed = true;
                    continue;
                }
                Owned<IPTreeIterator> msIt(node.getElements("maskStyle"));
                ForEach(*msIt)
                {
                    const IPTree& node = msIt->query();
                    const char* msName = node.queryProp("@name");
                    const char* msPresence = node.queryProp("@presence");
                    if (!checkHasMaskStyleForSelectedValueType(vtName, vtPresence, msName, msPresence))
                        failed = true;
                }
                Owned<IPTreeIterator> setIt(node.getElements("Set"));
                ForEach(*setIt)
                {
                    const IPTree& node = setIt->query();
                    const char* sName = node.queryProp("@name");
                    const char* sPresence = node.queryProp("@presence");
                    if (!checkSetMembershipForSelectedValueType(vtName, vtPresence, sName, sPresence))
                        failed = true;
                }
            }
            it.setown(requirements.getElements("rule"));
            ForEach(*it)
            {
                const IPTree& node = it->query();
                const char*   contentType = node.queryProp("@contentType");
                const char*   presence = node.queryProp("@presence");
                if (!checkHasRuleForContentType(contentType, presence))
                    failed = true;
            }
            return !failed;
        }
        bool checkUsingCustomProperty(const char* name, const char* value)
        {
            assertReady();
            if (!value)
                context->removeProperty(name);
            else if (!context->setProperty(name, value))
            {
                tracer->uerrlog("%s:%hhu: failed to set context property '%s' to '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), name, value);
                return false;
            }
            return true;
        }
        inline bool checkSupportsOperation(const char* name) const
        {
            return checkSupportsOperation(name, Required);
        }
        inline bool checkSupportsOperation(const char* name, const char* presence) const
        {
            return checkSupportsOperation(name, mapPresence(presence));
        }
        bool checkSupportsOperation(const char* name, Presence presence) const
        {
            assertReady();
            if (isEmptyString(name))
            {
                tracer->uwarnlog("%s:%hhu: empty operation name", context->inspector().queryDefaultDomain(), context->queryVersion());
            }
            else if (!streq(name, "maskValue"))
            {
                if (!checkCanMaskValue(presence))
                    return false;
            }
            else if (!streq(name, "maskContent"))
            {
                if (!checkCanMaskContent(presence))
                    return false;
            }
            else if (!streq(name, "maskMarkupValue"))
            {
                if (!checkCanMaskMarkupValue(presence))
                    return false;
            }
            else
            {
                tracer->uwarnlog("%s:%hhu: unknown operation name '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
            }
            return true;
        }
        inline bool checkCanMaskValue() const
        {
            return checkCanMaskValue(Required);
        }
        inline bool checkCanMaskValue(const char* presence) const
        {
            return checkCanMaskValue(mapPresence(presence));
        }
        bool checkCanMaskValue(Presence presence) const
        {
            assertReady();
            bool result = context->canMaskValue();
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: canMaskValue not supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: canMaskValue not supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: canMaskValue supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                    return false;
                }
                break;
            }
            return true;
        }
        inline bool checkCanMaskContent() const
        {
            return checkCanMaskContent(Required);
        }
        inline bool checkCanMaskContent(const char* presence) const
        {
            return checkCanMaskContent(mapPresence(presence));
        }
        bool checkCanMaskContent(Presence presence) const
        {
            assertReady();
            bool result = context->canMaskContent();
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: canMaskContent not supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: canMaskContent not supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: canMaskContent supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                    return false;
                }
                break;
            }
            return true;
        }
        inline bool checkCanMaskMarkupValue() const
        {
            return checkCanMaskMarkupValue(Required);
        }
        inline bool checkCanMaskMarkupValue(const char* presence) const
        {
            return checkCanMaskMarkupValue(mapPresence(presence));
        }
        bool checkCanMaskMarkupValue(Presence presence) const
        {
            assertReady();
            bool result = context->canMaskMarkupValue();
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: canMaskMarkupValue not supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: canMaskMarkupValue not supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: canMaskMarkupValue supported", context->inspector().queryDefaultDomain(), context->queryVersion());
                    return false;
                }
                break;
            }
            return true;
        }
        inline bool checkAcceptsProperty(const char* name) const
        {
            return checkAcceptsProperty(name, Required);
        }
        inline bool checkAcceptsProperty(const char* name, const char* presence) const
        {
            return checkAcceptsProperty(name, mapPresence(presence));
        }
        bool checkAcceptsProperty(const char* name, Presence presence) const
        {
            assertReady();
            bool result = context->inspector().acceptsProperty(name);
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: required property '%s' not accepted", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: optional property '%s' not accepted", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: prohibited property '%s' accepted", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                    return false;
                }
                break;
            }
            return true;
        }
        inline bool checkUsesProperty(const char* name) const
        {
            return checkUsesProperty(name, Required);
        }
        inline bool checkUsesProperty(const char* name, const char* presence) const
        {
            return checkUsesProperty(name, mapPresence(presence));
        }
        bool checkUsesProperty(const char* name, Presence presence) const
        {
            assertReady();
            bool result = context->inspector().usesProperty(name);
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: required property '%s' not used", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: optional property '%s' not used", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: prohibited property '%s' used", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                    return false;
                }
                break;
            }
            return true;
        }
        inline bool checkHasSelectedValueType(const char* name) const
        {
            return checkHasSelectedValueType(name, Required);
        }
        inline bool checkHasSelectedValueType(const char* name, const char* presence) const
        {
            return checkHasSelectedValueType(name, mapPresence(presence));
        }
        bool checkHasSelectedValueType(const char* name, Presence presence) const
        {
            assertReady();
            bool result = (context->inspector().queryValueType(name) != nullptr);
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: required value type '%s' not selected", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: optional value type '%s' not selected", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: progibited value type '%s' selected", context->inspector().queryDefaultDomain(), context->queryVersion(), name);
                    return false;
                }
                break;
            }
            return true;
        }
        inline bool checkHasMaskStyleForSelectedValueType(const char* vtName, const char* msName) const
        {
            return checkHasMaskStyleForSelectedValueType(vtName, Optional, msName, Required);
        }
        inline bool checkHasMaskStyleForSelectedValueType(const char* vtName, const char* vtPresence, const char* msName, const char* msPresence) const
        {
            return checkHasMaskStyleForSelectedValueType(vtName, mapPresence(vtPresence), msName, mapPresence(msPresence));
        }
        bool checkHasMaskStyleForSelectedValueType(const char* vtName, Presence vtPresence, const char* msName, Presence msPresence) const
        {
            assertReady();
            IDataMaskingProfileValueType* vt = context->inspector().queryValueType(vtName);
            if (!vt)
            {
                switch (vtPresence)
                {
                case Unknown:
                default:
                    tracer->uerrlog("%s:%hhu: unable to check mask style '%s'; value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    return false;
                case Required:
                    tracer->uerrlog("%s:%hhu: unable to check mask style '%s'; required value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    return false;
                case Optional:
                    tracer->uwarnlog("%s:%hhu: unable to check mask style '%s'; optional value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    return true;
                case Prohibited:
                    tracer->uwarnlog("%s:%hhu: unable to check mask style '%s'; prohibited value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    return true;
                }
            }
            else if (Prohibited == vtPresence)
            {
                tracer->uerrlog("%s:%hhu: found prohibited value type '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), vtName);
            }
            bool result = (vt->queryMaskStyle(context, msName) != nullptr);
            if (!result)
            {
                switch (msPresence)
                {
                case Unknown:
                default:
                    tracer->uerrlog("%s:%hhu: missing mask style '%s' for value type '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    return false;
                case Required:
                    tracer->uerrlog("%s:%hhu: missing required mask style '%s' for value type '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    return false;
                case Optional:
                    tracer->uwarnlog("%s:%hhu: missing optional mask style '%s' for value type '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                    break;
                case Prohibited:
                    break;
                }
            }
            else if (Prohibited == msPresence)
            {
                tracer->uerrlog("%s:%hhu: found prohibited mask style '%s' for value type '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), msName, vtName);
                return false;
            }
            return true;
        }
        inline bool checkSetMembershipForSelectedValueType(const char* vtName, const char* sName) const
        {
            return checkSetMembershipForSelectedValueType(vtName, Optional, sName, Required);
        }
        inline bool checkSetMembershipForSelectedValueType(const char* vtName, const char* vtPresence, const char* sName, const char* sPresence) const
        {
            return checkSetMembershipForSelectedValueType(vtName, mapPresence(vtPresence), sName, mapPresence(sPresence));
        }
        bool checkSetMembershipForSelectedValueType(const char* vtName, Presence vtPresence, const char* sName, Presence sPresence) const
        {
            assertReady();
            IDataMaskingProfileValueType* vt = context->inspector().queryValueType(vtName);
            if (!vt)
            {
                switch (vtPresence)
                {
                case Unknown:
                default:
                    tracer->uerrlog("%s:%hhu: unable to check membership in set '%s'; value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), sName, vtName);
                    return false;
                case Required:
                    tracer->uerrlog("%s:%hhu: unable to check membership in set '%s'; required value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), sName, vtName);
                    return false;;
                case Optional:
                    tracer->uwarnlog("%s:%hhu: unable to check membership in set '%s'; optional value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), sName, vtName);
                    return true;
                case Prohibited:
                    tracer->uwarnlog("%s:%hhu: unable to check membership in set '%s'; prohibited value type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), sName, vtName);
                    return true;
                }
            }
            else if (Prohibited == vtPresence)
            {
                tracer->uwarnlog("%s:%hhu: found prohibited value type '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), vtName);
            }
            bool result = vt->isMemberOf(sName);
            if (!result)
            {
                switch (sPresence)
                {
                case Unknown:
                default:
                    tracer->uerrlog("%s:%hhu: value type '%s' does not have membership in set '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), vtName, sName);
                    return false;
                case Required:
                    tracer->uerrlog("%s:%hhu: value type '%s' does not have required membership in set '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), vtName, sName);
                    return false;
                case Optional:
                    tracer->uwarnlog("%s:%hhu: value type '%s' does not have optional membership in set '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), vtName, sName);
                    break;
                case Prohibited:
                    break;
                }
            }
            else if (Prohibited == sPresence)
            {
                tracer->uerrlog("%s:%hhu: value type '%s' has prohibited membership in set '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), vtName, sName);
                return false;
            }
            return true;
        }
        inline bool checkHasRuleForContentType(const char* contentType) const
        {
            return checkHasRuleForContentType(contentType, Required);
        }
        inline bool checkHasRuleForContentType(const char* contentType, const char* presence) const
        {
            return checkHasRuleForContentType(contentType, mapPresence(presence));
        }
        bool checkHasRuleForContentType(const char* contentType, Presence presence) const
        {
            assertReady();
            bool result = context->inspector().hasRule(contentType);
            switch (presence)
            {
            case Unknown:
                return false;
            case Required:
                if (!result)
                {
                    tracer->uerrlog("%s:%hhu: required rule for content type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), contentType);
                    return false;
                }
                break;
            case Optional:
                if (!result)
                    tracer->uwarnlog("%s:%hhu: optional rule for content type '%s' not found", context->inspector().queryDefaultDomain(), context->queryVersion(), contentType);
                break;
            case Prohibited:
                if (result)
                {
                    tracer->uerrlog("%s:%hhu: prohibited rule for content type '%s' found", context->inspector().queryDefaultDomain(), context->queryVersion(), contentType);
                    return false;
                }
                break;
            }
            return true;
        }

    protected:
        inline void assertReady() const
        {
            if (!context)
            {
                tracer->ierrlog("missing context for compatibility check; call checkAndSetUsing first");
                throw makeStringException(-1, "compatibility checker not ready");
            }
        }
        Presence mapPresence(const char* presence) const
        {
            if (isEmptyString(presence) || strieq(presence, "r"))
                return Required;
            if (strieq(presence, "o"))
                return Optional;
            if (strieq(presence, "p"))
                return Prohibited;
            tracer->uerrlog("%s:%hhu: unknown presence value '%s'", context->inspector().queryDefaultDomain(), context->queryVersion(), presence);
            return Unknown;
        }
    };

} // namespace DataMasking
