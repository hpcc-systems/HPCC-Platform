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

/**
 * DFS file access audit context.
 *
 * DFSAuditContext is a lightweight, immutable-after-construction bag of
 * key/value string pairs that carries audit context through the call stack.
 *
 * A scoped context is created once (e.g. per workunit, per graph) with the
 * fields that are constant for that scope (component, user, wuid, graph, peer,
 * instance). Per-call extra fields are added via add(), which returns a new
 * context without modifying the original:
 *
 *   DFSAuditContext wuCtx({{"component","ECLAgent"},{"user","myuser"},
 *                           {"wuid","W001"},{"peer","10.0.0.1"}});
 *   DFSAuditContext callCtx = wuCtx.add({{"cluster","mythor"}});
 *
 * Serialisation:
 *   toLogfmt(buf)       — all pairs as "key=value key2=value2 ..."
 *
 * Audit record formatting (used by DFS when it emits ,FileAccess, records):
 *   buildAuditRecord()  — produces the full CSV line; well-known keys fill
 *                         the fixed CSV positions, remaining keys go into the
 *                         logfmt extras 9th field.
 *
 * Wire format:
 *   ws_dfsclient serialises via toLogfmt() and sends as a request field;
 *   ws_dfsservice deserialises via fromLogfmt() and reconstructs the context.
 */

#pragma once

#ifndef da_decl
#ifdef DALI_EXPORTS
#define da_decl DECL_EXPORT
#else
#define da_decl DECL_IMPORT
#endif
#endif

#include "jstring.hpp"
#include <initializer_list>
#include <utility>
#include <vector>

using LogfmtKV = std::pair<std::string, std::string>;
using LogfmtKVList = std::vector<LogfmtKV>;

class DFSAuditContext;
extern da_decl void setDefaultDFSAuditContext(const DFSAuditContext &ctx);
extern da_decl DFSAuditContext queryDefaultDFSAuditContext();
extern da_decl void initDFSAudit();
extern da_decl void closeDFSAudit();
//------------------------------------------------------------------------------
// DFSAuditContext
//------------------------------------------------------------------------------

class da_decl DFSAuditContext
{
public:
    using KVPair  = LogfmtKV;
    using KVList  = LogfmtKVList;

    DFSAuditContext() = default;
    explicit DFSAuditContext(const std::initializer_list<KVPair> _pairs);
    explicit DFSAuditContext(const KVList &_pairs);

    // Returns a new context with the extra pairs appended. Does not modify *this.
    DFSAuditContext add(std::initializer_list<KVPair> extra) const;

    // Returns a copy marked "nested". A nested context is passed to inner DFS
    // operations that are themselves audit emit points so they stay silent; the
    // outermost operation holds a non-nested context and owns the single
    // emission (and its verb). The marker is a dedicated member, deliberately
    // excluded from toLogfmt()/the wire form, so it never serialises or persists.
    DFSAuditContext nested() const;

    // True if this context was produced by nested(). Queried at each emit point:
    //   if (!auditCtx.isNested()) logFileAccess(verb);
    bool isNested() const { return nestedFlag; }

    // Serialise all pairs as logfmt: "key=value key2=value2 ..."
    // Used by ws_dfsclient to send context to ws_dfsservice.
    StringBuffer &toLogfmt(StringBuffer &out, const char *exclusions = nullptr) const;

    // Deserialise from logfmt produced by toLogfmt().
    // Used by ws_dfsservice to reconstruct the context from a request field.
    static DFSAuditContext fromLogfmt(const char *logfmt);

    const char *queryValue(const char *key, const char *defaultValue="") const;
    void setValue(const char *key, const char *value);

    // Build and optionally emit a complete FileAccess audit log line.
    // The line format is:
    //   ,FileAccess,<component>,<action>[,<logfmt-extras>]
    // where extras are the current context pairs with excluded keys removed.
    StringBuffer &buildFileAccessAuditLine(StringBuffer &out, const char *action);
    void logFileAccess(const char *action);

    const KVList &queryPairs() const { return pairs; }

    bool isEmpty() const { return pairs.empty(); }

private:
    KVList pairs;
    // Process-local suppression marker; NOT part of the logfmt/wire form.
    bool nestedFlag = false;
};

extern da_decl void buildClientInfoLogfmt(DFSAuditContext &context);
