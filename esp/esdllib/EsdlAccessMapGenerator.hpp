/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef _EsdlAccessMapGenerator_HPP_
#define _EsdlAccessMapGenerator_HPP_

#include "AccessMapGenerator.hpp"
#include "esdl_def.hpp"
#include "espcontext.hpp"
#include "jlog.hpp"
#include "jscm.hpp"
#include "seclib.hpp"

using EsdlAccessMapGenerator = TAccessMapGenerator<SecAccessFlags>;
using EsdlAccessMapScopeMapper = EsdlAccessMapGenerator::ScopeMapper;

struct EsdlAccessMapLevelMapper : public EsdlAccessMapGenerator::LevelMapper
{
    SecAccessFlags levelUnavailable() const override { return SecAccess_Unavailable; }
    SecAccessFlags levelNone() const override { return SecAccess_None; }
    SecAccessFlags levelDeferred() const override { return SecAccess_None; }
    SecAccessFlags levelAccess() const override { return SecAccess_Access; }
    SecAccessFlags levelRead() const override { return SecAccess_Read; }
    SecAccessFlags levelWrite() const override { return SecAccess_Write; }
    SecAccessFlags levelFull() const override { return SecAccess_Full; }
    SecAccessFlags levelUnknown() const override { return SecAccess_Unknown; }

    bool isEqual(SecAccessFlags lhs, SecAccessFlags rhs) const override
    {
        return lhs == rhs;
    }

    const char* toString(SecAccessFlags level) const override
    {
#define SECACCESSFLAGS_CASE(flag) case flag: return #flag
    switch (level)
    {
    SECACCESSFLAGS_CASE(SecAccess_Unavailable);
    SECACCESSFLAGS_CASE(SecAccess_None);
    SECACCESSFLAGS_CASE(SecAccess_Access);
    SECACCESSFLAGS_CASE(SecAccess_Read);
    SECACCESSFLAGS_CASE(SecAccess_Write);
    SECACCESSFLAGS_CASE(SecAccess_Full);
    default:
    SECACCESSFLAGS_CASE(SecAccess_Unknown);
    }
#undef SECACCESSFLAGS_CASE
    }
};

struct EsdlAccessMapReporter : public EsdlAccessMapGenerator::Reporter
{
    MapStringTo<SecAccessFlags>& m_accessMap;
    Owned<IEsdlDefReporter>      m_reporter;

    EsdlAccessMapReporter(MapStringTo<SecAccessFlags>& accessMap, IEsdlDefReporter* reporter)
        : m_accessMap(accessMap)
    {
        setEsdlReporter(reporter);
    }

    void setEsdlReporter(IEsdlDefReporter* reporter)
    {
        m_reporter.setown(reporter);
    }

    bool errorsAreFatal() const override
    {
        return true;
    }

    bool reportError() const override
    {
        return reportType(IEsdlDefReporter::ReportUError);
    }

    bool reportWarning() const override
    {
        return reportType(IEsdlDefReporter::ReportUWarning);
    }

    bool reportInfo() const override
    {
        return reportType(IEsdlDefReporter::ReportUInfo);
    }

    bool reportDebug() const override
    {
        return reportType(IEsdlDefReporter::ReportDInfo);
    }

    void entry(const char* name, SecAccessFlags level) const override
    {
        m_accessMap.setValue(name, level);
    }

protected:
#define REPORT_FLAGS(f) (f | IEsdlDefReporter::ReportMethod)
    void reportError(const char* fmt, va_list& args) const override  __attribute__((format(printf,2,0)))
    {
        StringBuffer msg;
        msg.valist_appendf(fmt, args);

        // The exception is thrown first due to a crash in the esdl application when the
        // exception occurs while fprintf is processing the message.
        if (errorsAreFatal())
            throw MakeStringException(0, "%s", msg.str());
        if (m_reporter.get() != nullptr)
            m_reporter->report(REPORT_FLAGS(IEsdlDefReporter::ReportUError), fmt, args);
    }

    void reportWarning(const char* fmt, va_list& args) const override
    {
        if (m_reporter.get() != nullptr)
            m_reporter->report(REPORT_FLAGS(IEsdlDefReporter::ReportUWarning), fmt, args);
    }

    void reportInfo(const char* fmt, va_list& args) const override
    {
        if (m_reporter.get() != nullptr)
            m_reporter->report(REPORT_FLAGS(IEsdlDefReporter::ReportUInfo), fmt, args);
    }

    void reportDebug(const char* fmt, va_list& args) const override
    {
        if (m_reporter.get() != nullptr)
            m_reporter->report(REPORT_FLAGS(IEsdlDefReporter::ReportDInfo), fmt, args);
    }

    bool reportType(IEsdlDefReporter::Flags flag) const
    {
        if (m_reporter.get() != nullptr)
            return m_reporter->testFlags(REPORT_FLAGS(flag));
        return false;
    }
#undef REPORT_FLAGS
};

#endif // _EsdlAccessMapGenerator_HPP_
