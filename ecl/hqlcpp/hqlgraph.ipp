/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef __HQLGRAPH_IPP_
#define __HQLGRAPH_IPP_

#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

#include "hqlhtcpp.ipp"
#include "hqltrans.ipp"

//---------------------------------------------------------------------------

class LogicalGraphInfo : public CInterface, public IInterface
{
public:
    LogicalGraphInfo(IHqlExpression * _expr) : expr(_expr) { globalId = 0; id = 0; subGraphId = 0; outputCount = 0; noDependents = false; }
    IMPLEMENT_IINTERFACE

public:
    HqlExprAttr expr;
    unique_id_t globalId;
    unique_id_t id;
    unique_id_t subGraphId;
    unsigned outputCount;
    bool noDependents;
};



typedef IArrayOf<IPropertyTree> PropertyTreeArray;

class LogicalGraphCreator
{
public:
    LogicalGraphCreator(IWorkUnit * _wu);
    ~LogicalGraphCreator();

    void createLogicalGraph(HqlExprArray & exprs);
    unsigned queryMaxActivityId() { return seq; }

protected:
    void connectActivities(IHqlExpression * from, IHqlExpression * to, IAtom * kind, const char * comment = NULL, bool nWay = false);
    void createRootGraphActivity(IHqlExpression * expr);
    void createGraphActivity(IHqlExpression * expr);
    void createSubGraphActivity(IHqlExpression * expr);
    bool gatherGraphActivities(IHqlExpression * expr, HqlExprArray & dependents);
    void gatherWorkflowActivities(IHqlExpression * expr, HqlExprArray & dependents);

    void beginSubGraph(const char * label, bool nested);
    void endSubGraph(bool nested);
    void beginActivity(const char * label, unique_id_t id);
    void endActivity();

    bool inSubQuery() const;
    bool isWorkflowExpanded(IHqlExpression * expr) const;

    void addAttribute(const char * name, const char * value);
    void addAttribute(const char * name, IAtom * value);
    void addAttributeInt(const char * name, __int64 value);
    void addAttributeBool(const char * name, bool value);

    void saveSubGraphs();
    void restoreSubGraphs();

    const char * getActivityText(IHqlExpression * expr, StringBuffer & temp);
    LogicalGraphInfo * queryExtra(IHqlExpression * expr);
    IPropertyTree * curSubGraph();

protected:
    IWorkUnit * wu;
    Owned<IPropertyTree> graph;
    Owned<IPropertyTree> rootSubGraph;
    Owned<IPropertyTree> activityNode;
    PropertyTreeArray saved;
    PropertyTreeArray subGraphs;
    UnsignedArray savedLevels;
    UnsignedArray savedGraphId;
    unsigned seq;
    unsigned rootGraphId;
    unsigned subGraphId;
    //Different configuration options
    bool expandPersist;
    bool expandStored;
    bool includeNameInText;
    bool includeModuleInText;
    bool displayJavadoc;
    bool displayJavadocParameters;
};

//---------------------------------------------------------------------------

IPropertyTree * addGraphAttribute(IPropertyTree * node, const char * name);
void addGraphAttribute(IPropertyTree * node, const char * name, const char * value);
void addGraphAttributeInt(IPropertyTree * node, const char * name, __int64 value);
void addGraphAttributeBool(IPropertyTree * node, const char * name, bool value);
IPropertyTree * addSimpleGraphEdge(IPropertyTree * subGraph, unsigned __int64 source, unsigned __int64 target, unsigned outputIndex, unsigned inputIndex, IAtom * kind, const char * label, bool nWay);
IPropertyTree * addComplexGraphEdge(IPropertyTree * graph, unsigned __int64 sourceGraph, unsigned __int64 targetGraph, unsigned __int64 sourceActivity, unsigned __int64 targetActivity, unsigned outputIndex, IAtom * kind, const char * label);
void removeGraphAttribute(IPropertyTree * node, const char * name);

#endif
