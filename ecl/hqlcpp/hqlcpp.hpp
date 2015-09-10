/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
"globalFoldOptions"      0xffff - options for global folding.
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
"peephole"               true   - peephole optimize memcpy/memsets etc.
"spotCSE"                true   - Look for common sub-expressions in transforms/filters?
"spotTopN"               true   - Convert choosen(sort()) into a topN activity.
"spotLocalMerge"         false  - if local join, and both sides sorted generate a light-weight merge
"allowThroughSpill"      true   - allow through spills.
"spillMultiCondition"    false  - another roxie variant.
"spotThroughAggregate"   true   - whether aggregates are done as through aggregates (generally more efficient)

4. Temporary flags
"optimizeBoolReturn"     true   - improve code when returning boolean from a function

*/

#define DEFAULT_conditionalStatements false

#define STRING_RESOURCES

#define CREATE_DEAULT_ROW_IF_NULL

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
    virtual HqlStmts * ensureSection(IAtom * section) = 0;
    virtual const char * queryLibrary(unsigned idx) = 0;
    virtual const char * queryObjectFile(unsigned idx) = 0;
    virtual const char * querySourceFile(unsigned idx) = 0;
    virtual HqlStmts * querySection(IAtom * section) = 0;
    virtual void addResource(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry, unsigned id=(unsigned)-1) = 0;
    virtual void addCompressResource(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry, unsigned id=(unsigned)-1) = 0;
    virtual void addManifest(const char *filename) = 0;
    virtual void addManifestFromArchive(IPropertyTree *archive) = 0;
    virtual void addWebServiceInfo(IPropertyTree *wsinfo) = 0;
    virtual void flushHints() = 0;
    virtual void flushResources(const char *filename, ICodegenContextCallback * ctxCallback) = 0;
};

// A class used to hold the context about the expression being processed at this point in time.
// Note: expr is updated as the query is processed to ensure that the parsed tree etc. get freed up early.
class HQLCPP_API HqlQueryContext
{
public:
    OwnedHqlExpr expr;  // Modified as the query is processed.
};

interface HQLCPP_API IHqlCppTranslator : public IInterface
{
public:
    virtual bool buildCpp(IHqlCppInstance & _code, HqlQueryContext & query) = 0;
};

extern HQLCPP_API IHqlCppInstance * createCppInstance(IWorkUnit * wu, const char * wupathname);
extern HQLCPP_API IHqlExpression * ensureIndexable(IHqlExpression * expr);

extern HQLCPP_API bool isChildOf(IHqlExpression * parent, IHqlExpression * child);
extern HQLCPP_API bool isProjectedInRecord(IHqlExpression * record, IHqlExpression * child);

extern HQLCPP_API IHqlExpression * adjustValue(IHqlExpression * value, __int64 delta);
extern HQLCPP_API bool matchesConstValue(IHqlExpression * expr, __int64 matchValue);
extern HQLCPP_API IHqlExpression * createTranslated(IHqlExpression * expr);
extern HQLCPP_API IHqlExpression * createTranslatedOwned(IHqlExpression * expr);
extern HQLCPP_API IHqlExpression * createTranslated(IHqlExpression * expr, IHqlExpression * length);
extern HQLCPP_API IHqlExpression * getSimplifyCompareArg(IHqlExpression * expr);

#endif
