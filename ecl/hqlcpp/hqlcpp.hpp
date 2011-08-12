/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#ifndef __HQLCPP_HPP_
#define __HQLCPP_HPP_

#ifdef HQLCPP_EXPORTS
#define HQLCPP_API __declspec(dllexport)
#else
#define HQLCPP_API __declspec(dllimport)
#endif

#include "jlib.hpp"
#include "hqlexpr.hpp"
#include "workunit.hpp"

/*
The following debug options are currently supported by the code generator:

1. Generally useful
"checkRowOverflow"       true   - check for potential overflows of records...
"expandRepeatAnyAsDfa"   true   - is ANY* expanded in a DFA
"forceFakeThor"          false  - force code to use hthor.
"freezePersists"         false  - stop persists being recreated.
"globalFold"             true   - perform a global constant fold before generating
    "globalFoldOptions"         - options for above.
"globalOptimize"         false  - perform a global optimize?
"groupAllDistribute"     false  - does group,all generate a distribute or a global sort?
"maximizeLexer"          false  - maximize the amount of work done in the lexer (?default on?)
"maxLength"              4096   - default maximum length of a record, unless specified.
"minimizeSpillSize",     false  - if a spill is filter/deduped etc when read, reduce the spill file size by splitting, filtering and then writing.
"optimizeGraph"          true   - optimize expressions in a graph before generation
"orderDiskFunnel"        true   - if all inputs to a funnel are disk reads, pull in order.
"parseDfaComplexity"     2000   - maximum complexity of expression to convert to a DFA.
"pickBestEngine"         true   - use hthor if it is more efficient than thor
"sortIndexPayload"       true   - do we sort by the non-keyed elements in an index.
"targetClusterType"      ----   - hthor|thor|roxie - who are we generating code for?
"topnLimit"              10000  - maximum number of records to do topN on.
"traceRowXml"            false  - add information to allow rows to be traced as XML in the meta information.

2. Useful for debugging.
"clusterSize"            ----   - override the number of nodes in the cluster (for testing)
"debugNlp"               false  - output debug information about the NLP processing to the .cpp file.
"resourceMaxMemory"     ?400M   - maximum amount of memory a subgraph can use.
"resourceMaxSockets"     2000   - maximum number of sockets a subgraph can use
"resourceMaxActivities"  200    - maximum number of activities a subgraph can contain
"unlimitedResources"     false  - assume lots of resources when resourcing the graphs.

3. Minor code generation tweaks
"filteredReadSpillThreshold" 2  - filtered disk reads are spilt if will be duplicated more than N times.
"foldConstantCast"       true   - Is (cast)value folded at generate time?
"foldFilter"             true   - should filters be constant folded?
"foldAssign"             true   - should transforms be constant folded?
"foldGraph"              false  - fold expressions at the graph level if optimizeGraph is set.
"foldOptimized"          false  - fold expressions when internal optimizer called.
"optimizeThorCounts"     true   - convert count(diskfile) into optimized version.
"peephole"               true   - peephole optimize memcpy/memsets etc.
"spotCSE"                true   - Look for common sub-expressions in transforms/filters?
"spotTopN"               true   - Convert choosen(sort()) into a topN activity.
"spotLocalMerge"         false  - if local join, and both sides sorted generate a light-weight merge
"countIndex"             false  - optimize count(index) into optimized version. (also requires optimizeThorCounts)
"allowThroughSpill"      true   - allow through spills.
"spillMultiCondition"    false  - another roxie variant.
"spotThroughAggregate"   true   - whether aggregates are done as through aggregates (generally more efficient)

4. Temporary flags
"optimizeBoolReturn"     true   - improve code when returning boolean from a function

*/

#define DEFAULT_conditionalStatements false

enum ClusterType { NoCluster, ThorCluster, HThorCluster, RoxieCluster, ThorLCRCluster };

extern HQLCPP_API ClusterType getClusterType(const char * platform, ClusterType dft = NoCluster);
inline bool isThorCluster(ClusterType type) { return (type == ThorCluster) || (type == ThorLCRCluster); }

#define STRING_RESOURCES

#define CREATE_DEAULT_ROW_IF_NULL

interface ICodegenContextCallback : public IInterface
{
    virtual void noteCluster(const char *clusterName) = 0;
    virtual void registerFile(const char * filename, const char * description) = 0;
    virtual bool allowAccess(const char * category) = 0;
};

//interface that represents a implementation of a query
//ready for passing to the thing that executes it.
interface HQLCPP_API IHqlQueryInstance : public IInterface
{
public:
    virtual void serialise(MemoryBuffer & out) = 0;
    virtual void serialiseDll(MemoryBuffer & out) = 0;
};


interface IHqlDelayedCodeGenerator : public IInterface
{
    virtual void generateCpp(StringBuffer & out) = 0;
};

interface IFunctionDatabase;

class HqlStmts;
interface HQLCPP_API IHqlCppInstance : public IInterface
{
public:
    virtual HqlStmts * ensureSection(_ATOM section) = 0;
    virtual const char * queryLibrary(unsigned idx) = 0;
    virtual HqlStmts * querySection(_ATOM section) = 0;
    virtual void addResource(const char * type, unsigned len, const void * data, IPropertyTree *entryEx, unsigned id=(unsigned)-1) = 0;
    virtual void addCompressResource(const char * type, unsigned len, const void * data, IPropertyTree *entryEx, unsigned id=(unsigned)-1) = 0;
    virtual void addManifest(const char *filename) = 0;
    virtual void addManifestFromArchive(IPropertyTree *archive) = 0;
    virtual void flushHints() = 0;
    virtual void flushResources(const char *filename, ICodegenContextCallback * ctxCallback) = 0;
};

interface HQLCPP_API IHqlCppTranslator : public IInterface
{
public:
    virtual bool buildCpp(IHqlCppInstance & _code, IHqlExpression * expr) = 0;
};

extern HQLCPP_API IHqlCppInstance * createCppInstance(IWorkUnit * wu, const char * wupathname);
extern HQLCPP_API IHqlExpression * ensureIndexable(IHqlExpression * expr);

extern HQLCPP_API bool isChildOf(IHqlExpression * parent, IHqlExpression * child);
extern HQLCPP_API bool isProjectedInRecord(IHqlExpression * record, IHqlExpression * child);
extern HQLCPP_API IHqlExpression * queryStripCasts(IHqlExpression * expr);

extern HQLCPP_API IHqlExpression * adjustValue(IHqlExpression * value, __int64 delta);
extern HQLCPP_API bool matchesConstValue(IHqlExpression * expr, __int64 matchValue);
extern HQLCPP_API IHqlExpression * createTranslated(IHqlExpression * expr);
extern HQLCPP_API IHqlExpression * createTranslatedOwned(IHqlExpression * expr);
extern HQLCPP_API IHqlExpression * createTranslated(IHqlExpression * expr, IHqlExpression * length);
extern HQLCPP_API IHqlExpression * getSimplifyCompareArg(IHqlExpression * expr);

#endif
