/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "platform.h"
#include "dafsaudit.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jthread.hpp"
#include "jstring.hpp"

#include <memory>
#include <string_view>

static void appendLogfmtPair(StringBuffer &out, const char *key, const char *value, bool &first)
{
    if (!value)
        value = "";

    if (!first)
        out.append(' ');
    first = false;

    bool needsQuote = strpbrk(value, " ,\"=\t\n\r") != nullptr;
    out.append(key).append("=");
    if (needsQuote)
    {
        out.append('"');
        for (const char *p = value; *p; p++)
        {
            if (*p == '\\')
                out.append("\\\\");
            else if (*p == '"')
                out.append("\\\"");
            else
                out.append(*p);
        }
        out.append('"');
    }
    else
        out.append(value);
}

static void parseLogfmtToKVList(const char *logfmt, LogfmtKVList &output)
{
    output.clear();
    if (isEmptyString(logfmt))
        return;

    const char *p = logfmt;
    while (*p)
    {
        while (*p == ' ')
            p++;
        if (!*p)
            break;

        const char *keyStart = p;
        while (*p && *p != '=')
            p++;
        if (!*p)
            break;

        std::string_view keyView(keyStart, static_cast<size_t>(p - keyStart));
        p++;

        std::string_view valueView;
        bool hasEscape = false;
        if (*p == '"')
        {
            p++;
            const char *valueStart = p;
            while (*p && *p != '"')
            {
                if (*p == '\\' && (*(p + 1) == '"' || *(p + 1) == '\\'))
                {
                    hasEscape = true;
                    p += 2;
                }
                else
                    p++;
            }
            valueView = std::string_view(valueStart, static_cast<size_t>(p - valueStart));

            if (*p == '"')
                p++;
        }
        else
        {
            const char *valueStart = p;
            while (*p && *p != ' ')
                p++;
            valueView = std::string_view(valueStart, static_cast<size_t>(p - valueStart));
        }

        std::string value;
        if (hasEscape)
        {
            value.reserve(valueView.size());
            const char *start = valueView.data();
            const char *end = start + valueView.size();
            for (const char *cur = start; cur < end; ++cur)
            {
                if (*cur == '\\' && (cur + 1) < end)
                {
                    char next = *(cur + 1);
                    if (next == '"' || next == '\\')
                    {
                        value.push_back(next);
                        ++cur;
                    }
                    else
                        value.push_back(*cur);
                }
                else
                    value.push_back(*cur);
            }
        }
        else
            value.assign(valueView.data(), valueView.size());

        output.emplace_back(std::string(keyView), std::move(value));
    }
}

static const char *queryLogfmtValue(const LogfmtKVList &items, const char *key, const char *defaultValue)
{
    if (isEmptyString(key))
        return defaultValue;

    for (const auto &item : items)
    {
        if (item.first == key)
            return item.second.c_str();
    }
    return defaultValue;
}

static void logfmtKVListToString(const LogfmtKVList &items, StringBuffer &output, const char *exclusions)
{
    std::vector<std::string_view> exclusionList;
    if (!isEmptyString(exclusions))
    {
        const char *p = exclusions;
        while (true)
        {
            const char *start = p;
            while (*p && *p != ',')
                p++;
            if (p > start)
                exclusionList.emplace_back(start, static_cast<size_t>(p - start));
            if (*p == '\0')
                break;
            p++;
        }
    }

    auto isExcluded = [&exclusionList](const char *key) -> bool
    {
        std::string_view keyView(key, strlen(key));
        for (const auto &item : exclusionList)
        {
            if (item == keyView)
                return true;
        }
        return false;
    };

    bool first = true;
    for (const auto &item : items)
    {
        if (!exclusionList.empty() && isExcluded(item.first.c_str()))
            continue;
        if (item.second.empty())
            continue;
        appendLogfmtPair(output, item.first.c_str(), item.second.c_str(), first);
    }
}

//------------------------------------------------------------------------------
// DFSAuditContext
//------------------------------------------------------------------------------

static ReadWriteLock defaultAuditContextRWLock{SYNC_LOCATION};
static DFSAuditContext globalDefaultAuditContext;

void setDefaultDFSAuditContext(const DFSAuditContext &ctx)
{
    WriteLockBlock block(defaultAuditContextRWLock);
    globalDefaultAuditContext = ctx;
}

DFSAuditContext queryDefaultDFSAuditContext()
{
    // Take a locked snapshot so callers never observe concurrent mutation.
    ReadLockBlock block(defaultAuditContextRWLock);
    return globalDefaultAuditContext;
}

DFSAuditContext::DFSAuditContext(const std::initializer_list<KVPair> _pairs)
    : pairs(_pairs)
{
}

DFSAuditContext::DFSAuditContext(const KVList &_pairs)
    : pairs(_pairs)
{
}

DFSAuditContext DFSAuditContext::add(const std::initializer_list<KVPair> extra) const
{
    KVList merged = pairs;
    for (const auto &kv : extra)
    {
        if (!kv.first.empty())
            merged.push_back(kv);
    }
    return DFSAuditContext(merged);
}

DFSAuditContext DFSAuditContext::nested() const
{
    DFSAuditContext copy(*this);
    copy.nestedFlag = true;
    return copy;
}

const char *DFSAuditContext::queryValue(const char *key, const char *defaultValue) const
{
    return queryLogfmtValue(pairs, key, defaultValue);
}

void DFSAuditContext::setValue(const char *key, const char *value)
{
    if (isEmptyString(key))
        return;

    for (auto &item : pairs)
    {
        if (item.first == key)
        {
            item.second = value ? value : "";
            return;
        }
    }

    pairs.emplace_back(key, value ? value : "");
}

StringBuffer &DFSAuditContext::toLogfmt(StringBuffer &out, const char *exclusions) const
{
    logfmtKVListToString(pairs, out, exclusions);
    return out;
}

StringBuffer &DFSAuditContext::buildFileAccessAuditLine(StringBuffer &out, const char *action)
{
    constexpr const char *fixedFieldExclusions = "component";
    assertex(action);

    // Merge with process default context to ensure base fields (e.g. component) are always present
    DFSAuditContext effectiveContext = queryDefaultDFSAuditContext();
    for (const auto &kv : pairs)
        effectiveContext.setValue(kv.first.c_str(), kv.second.c_str());

    const char *component = effectiveContext.queryValue("component", "Unknown");
    out.appendf(",FileAccess,%s,%s", component, action);

    // Append extras directly into the output to avoid a temporary buffer;
    // if nothing is emitted after exclusions, remove the separator comma.
    size32_t checkpoint = out.length();
    out.append(',');
    effectiveContext.toLogfmt(out, fixedFieldExclusions);
    if (out.length() == checkpoint + 1)
        out.setLength(checkpoint);

    return out;
}

void DFSAuditContext::logFileAccess(const char *action)
{
    StringBuffer auditLine;
    buildFileAccessAuditLine(auditLine, action);
    LOG(MCauditInfo, "%s", auditLine.str());
}

DFSAuditContext DFSAuditContext::fromLogfmt(const char *logfmt)
{
    KVList pairs;
    parseLogfmtToKVList(logfmt, pairs);
    return DFSAuditContext(pairs);
}

void buildClientInfoLogfmt(DFSAuditContext &context)
{
    static const std::string cachedDeploymentName = []()
    {
        std::unique_ptr<char, decltype(&free)> val(getHPCCEnvVal("HPCC_DEPLOYMENT", "deployment-name-not-configured"), &free);
        return std::string(val.get() ? val.get() : "");
    }();

    if (!cachedDeploymentName.empty())
        context.setValue("deployment", cachedDeploymentName.c_str());

    const char *job = queryLogMsgManager()->queryJobId(queryThreadedJobId());
    if (!isEmptyString(job))
        context.setValue("job", job);
}

//------------------------------------------------------------------------------
// Global hook for populating the baseline context when config loads
//------------------------------------------------------------------------------

static unsigned dfsAuditHookId = 0;

void initDFSAudit()
{
    if (!dfsAuditHookId)
    {
        auto updateFunc = [&](const IPropertyTree *compCfg, const IPropertyTree *globalCfg)
        {
            DFSAuditContext baseCtx;

            const char *compTag = compCfg->queryName();
            if (!isEmptyString(compTag))
                baseCtx.setValue("component", compTag);

            const char *instName = globalCfg->queryProp("@name");
            if (!isEmptyString(instName))
                baseCtx.setValue("instance", instName);

            setDefaultDFSAuditContext(baseCtx);
        };

        dfsAuditHookId = installConfigUpdateHook(updateFunc, true);
    }
}

void closeDFSAudit()
{
    if (dfsAuditHookId)
    {
        removeConfigUpdateHook(dfsAuditHookId);
        dfsAuditHookId = 0;
    }
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class DFSAuditContextTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DFSAuditContextTests);
        CPPUNIT_TEST(testAddIsCopyOnWrite);
        CPPUNIT_TEST(testFromLogfmtRoundtrip);
        CPPUNIT_TEST(testEmptyValuesOmittedFromLogfmt);
        CPPUNIT_TEST(testBuildFileAccessAuditLine);
        CPPUNIT_TEST(testNestedFlagInvisibleOnWire);
    CPPUNIT_TEST_SUITE_END();

public:
    // add() returns a new context; original is unchanged, new context has all pairs
    void testAddIsCopyOnWrite()
    {
        DFSAuditContext base({{"component","Thor"},{"user","u1"}});
        DFSAuditContext extended = base.add({{"cluster","mythor"},{"peer","10.0.0.1"}});

        StringBuffer baseBuf, extBuf;
        base.toLogfmt(baseBuf);
        extended.toLogfmt(extBuf);

        CPPUNIT_ASSERT(strstr(baseBuf.str(), "cluster") == nullptr);
        CPPUNIT_ASSERT(strstr(extBuf.str(), "cluster=mythor") != nullptr);
        CPPUNIT_ASSERT(strstr(extBuf.str(), "component=Thor") != nullptr);
    }

    // toLogfmt/fromLogfmt round-trip preserves all pairs
    void testFromLogfmtRoundtrip()
    {
        DFSAuditContext orig({{"component","DFU"},{"user","svc"},{"peer","1.2.3.4"},{"wuid","W999"}});
        StringBuffer wire;
        orig.toLogfmt(wire);
        DFSAuditContext restored = DFSAuditContext::fromLogfmt(wire.str());
        StringBuffer out;
        restored.toLogfmt(out);
        CPPUNIT_ASSERT(strstr(out.str(), "component=DFU") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "peer=1.2.3.4") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "wuid=W999") != nullptr);
    }

    // Empty values are silently omitted from toLogfmt
    void testEmptyValuesOmittedFromLogfmt()
    {
        DFSAuditContext ctx({{"component","ECLAgent"},{"wuid",""},{"user","u"}});
        StringBuffer out;
        ctx.toLogfmt(out);
        CPPUNIT_ASSERT(strstr(out.str(), "wuid") == nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "component=ECLAgent") != nullptr);
    }

    // buildFileAccessAuditLine uses caller-provided component and excludes fixed fields from extras
    void testBuildFileAccessAuditLine()
    {
        DFSAuditContext ctx({{"user","alice"},{"lfn","scope::name"}});
        ctx.setValue("component", "Foreign");
        StringBuffer out;
        ctx.buildFileAccessAuditLine(out, "READ");

        CPPUNIT_ASSERT(strstr(out.str(), ",FileAccess,Foreign,READ") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "user=alice") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "lfn=scope::name") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "component=") == nullptr);
    }

    // nested() sets a process-local marker that must NOT affect serialisation:
    // a nested context and its non-nested twin produce byte-identical logfmt,
    // and the marker does not survive a fromLogfmt() round-trip (never crosses
    // the wire).
    void testNestedFlagInvisibleOnWire()
    {
        DFSAuditContext base({{"component","Thor"},{"user","u1"},{"lfn","scope::name"}});
        DFSAuditContext nestedCtx = base.nested();

        // nested() does not mutate the original and does set the marker on the copy
        CPPUNIT_ASSERT(!base.isNested());
        CPPUNIT_ASSERT(nestedCtx.isNested());

        StringBuffer baseBuf, nestedBuf;
        base.toLogfmt(baseBuf);
        nestedCtx.toLogfmt(nestedBuf);
        CPPUNIT_ASSERT(streq(baseBuf.str(), nestedBuf.str()));

        // The marker is process-local: a context reconstructed from the wire is
        // never nested.
        DFSAuditContext restored = DFSAuditContext::fromLogfmt(nestedBuf.str());
        CPPUNIT_ASSERT(!restored.isNested());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(DFSAuditContextTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(DFSAuditContextTests, "DFSAuditContextTests");

#endif // _USE_CPPUNIT
