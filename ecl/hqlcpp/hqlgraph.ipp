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
    void connectActivities(IHqlExpression * from, IHqlExpression * to, _ATOM kind, const char * comment = NULL, bool nWay = false);
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
    void addAttribute(const char * name, _ATOM value);
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
void addSimpleGraphEdge(IPropertyTree * subGraph, unsigned __int64 source, unsigned __int64 target, unsigned outputIndex, unsigned inputIndex, _ATOM kind, const char * label, bool nWay);
void addComplexGraphEdge(IPropertyTree * graph, unsigned __int64 sourceGraph, unsigned __int64 targetGraph, unsigned __int64 sourceActivity, unsigned __int64 targetActivity, unsigned outputIndex, _ATOM kind, const char * label);
void removeGraphAttribute(IPropertyTree * node, const char * name);

#endif
