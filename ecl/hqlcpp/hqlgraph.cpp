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
#include "jliball.hpp"
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"

#include "hql.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlutil.hpp"
#include "hqlthql.hpp"
#include "hqlpmap.hpp"

#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlopt.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlgraph.ipp"
#include "thorcommon.hpp"

//---------------------------------------------------------------------------

IPropertyTree * addGraphAttribute(IPropertyTree * node, const char * name)
{
    IPropertyTree * att = createPTree();
    att->setProp("@name", name);
    return node->addPropTree("att", att);
}

void addGraphAttribute(IPropertyTree * node, const char * name, const char * value)
{
    addGraphAttribute(node, name)->setProp("@value", value);
}

void addGraphAttributeInt(IPropertyTree * node, const char * name, __int64 value)
{
    addGraphAttribute(node, name)->setPropInt64("@value", value);
}

void addGraphAttributeBool(IPropertyTree * node, const char * name, bool value)
{
    if (value)
        addGraphAttribute(node, name)->setPropBool("@value", value);
}

IPropertyTree * addIntraGraphEdge(IPropertyTree * subGraph, unsigned __int64 source, unsigned __int64 target, unsigned outputIndex)
{
    IPropertyTree *edge = createPTree();
    edge->setPropInt64("@target", target);
    edge->setPropInt64("@source", source);

    StringBuffer s;
    edge->setProp("@id", s.append(source).append('_').append(outputIndex).str());
    return subGraph->addPropTree("edge", edge);
}

IPropertyTree * addInterGraphEdge(IPropertyTree * graph, unsigned __int64 sourceGraph, unsigned __int64 targetGraph, unsigned __int64 sourceActivity, unsigned __int64 targetActivity, unsigned outputIndex)
{
    StringBuffer idText;
    IPropertyTree *edge = createPTree();
    edge->setProp("@id", idText.clear().append(sourceGraph).append('_').append(targetGraph).append("_").append(outputIndex).str());
    edge->setPropInt64("@target", sourceGraph);
    edge->setPropInt64("@source", targetGraph);

    addGraphAttributeInt(edge, "_sourceActivity", sourceActivity);
    addGraphAttributeInt(edge, "_targetActivity", targetActivity);
    return graph->addPropTree("edge", edge);
}

void setEdgeAttributes(IPropertyTree * edge, unsigned outputIndex, unsigned inputIndex, IAtom * kind, const char * label, bool nWay)
{
    if (outputIndex != 0)
        addGraphAttributeInt(edge, "_sourceIndex", outputIndex);
    if (inputIndex != 0)
        addGraphAttributeInt(edge, "_targetIndex", inputIndex);
    if (label)
        edge->setProp("@label", label);
    if (kind == dependencyAtom)
        addGraphAttributeBool(edge, "_dependsOn", true);
    if (nWay)
        edge->setPropBool("@nWay", true);
}


IPropertyTree * addSimpleGraphEdge(IPropertyTree * subGraph, unsigned __int64 source, unsigned __int64 target, unsigned outputIndex, unsigned inputIndex, IAtom * kind, const char * label, bool nWay)
{
    IPropertyTree *edge = addIntraGraphEdge(subGraph, source, target, outputIndex);
    setEdgeAttributes(edge, outputIndex, inputIndex, kind, label, nWay);
    return edge;
}

IPropertyTree * addComplexGraphEdge(IPropertyTree * graph, unsigned __int64 sourceGraph, unsigned __int64 targetGraph, unsigned __int64 sourceActivity, unsigned __int64 targetActivity, unsigned outputIndex, IAtom * kind, const char * label)
{
    IPropertyTree * edge = addInterGraphEdge(graph, sourceGraph, targetGraph, sourceActivity, targetActivity, outputIndex);
    setEdgeAttributes(edge, outputIndex, 0, kind, label, false);
    return edge;
}


void removeGraphAttribute(IPropertyTree * node, const char * name)
{
    StringBuffer xpath;
    xpath.append("att[@name=\"").append(name).append("\"]");
    node->removeProp(xpath.str());
}


static void queryExpandFilename(StringBuffer & out, IHqlExpression * expr)
{
    if (expr)
    {
        OwnedHqlExpr folded = foldHqlExpression(expr);
        if (folded->queryValue())
            folded->queryValue()->generateECL(out.append('\n'));
    }
}


//---------------------------------------------------------------------------

static void addDependent(HqlExprArray & dependents, IHqlExpression * expr)
{
    if (dependents.find(*expr) == NotFound)
        dependents.append(*LINK(expr));
}

LogicalGraphCreator::LogicalGraphCreator(IWorkUnit * _wu)
{
    lockTransformMutex();
    seq = 0;
    wu = _wu;
    expandPersist = wu->getDebugValueBool("logicalGraphExpandPersist", true);
    expandStored = wu->getDebugValueBool("logicalGraphExpandStored", false);
    includeNameInText = wu->getDebugValueBool("logicalGraphIncludeName", true);
    includeModuleInText = wu->getDebugValueBool("logicalGraphIncludeModule", true);
    displayJavadoc = wu->getDebugValueBool("logicalGraphDisplayJavadoc", true);
    displayJavadocParameters = wu->getDebugValueBool("logicalGraphDisplayJavadocParameters", false);
    rootGraphId = 0;
    subGraphId = 0;
}

LogicalGraphCreator::~LogicalGraphCreator()
{
    unlockTransformMutex();
}

void LogicalGraphCreator::addAttribute(const char * name, const char * value)
{
    if (value)
        addGraphAttribute(activityNode, name, value);
}

void LogicalGraphCreator::addAttribute(const char * name, IAtom * value)
{
    if (value)
        addGraphAttribute(activityNode, name, value->str());
}

void LogicalGraphCreator::addAttributeInt(const char * name, __int64 value)
{
    addGraphAttributeInt(activityNode, name, value);
}

void LogicalGraphCreator::addAttributeBool(const char * name, bool value)
{
    addGraphAttributeBool(activityNode, name, value);
}

void LogicalGraphCreator::beginActivity(const char * label, unique_id_t id)
{
    activityNode.set(curSubGraph()->addPropTree("node", createPTree()));
    if (label)
        activityNode->setProp("@label", label);
    activityNode->setPropInt64("@id", id);
}

void LogicalGraphCreator::beginSubGraph(const char * label, bool nested)
{
    savedGraphId.append(subGraphId);
    if (!nested)
        saveSubGraphs();

    if ((subGraphs.ordinality() == 0) && rootSubGraph)
    {
        subGraphs.append(*LINK(rootSubGraph));
        subGraphId = rootGraphId;
        return;
    }

    subGraphId = ++seq;
    IPropertyTree * node = createPTree("node");
    node = curSubGraph()->addPropTree("node", node);
    node->setPropInt64("@id", subGraphId);

    IPropertyTree * graphAttr = node->addPropTree("att", createPTree("att"));
    IPropertyTree * subGraph = graphAttr->addPropTree("graph", createPTree("graph"));
    subGraphs.append(*LINK(subGraph));
    if (!rootSubGraph)
    {
        rootSubGraph.set(subGraph);
        rootGraphId = subGraphId;
    }
}

void LogicalGraphCreator::connectActivities(IHqlExpression * fromExpr, IHqlExpression * toExpr, IAtom * kind, const char * label, bool nWay)
{
    StringBuffer tempLabel;
    if (fromExpr->getOperator() == no_comma)
    {
        fromExpr->queryChild(1)->queryValue()->getStringValue(tempLabel);
        fromExpr = fromExpr->queryChild(0);
        label = tempLabel.str();
    }
    LogicalGraphInfo * from = queryExtra(fromExpr);
    LogicalGraphInfo * to = queryExtra(toExpr);
    if (kind == dependencyAtom)
    {
        LogicalGraphInfo * temp = from;
        from = to;
        to = temp;
    }

    if (from->subGraphId == to->subGraphId)
        addSimpleGraphEdge(curSubGraph(), from->id, to->id, from->outputCount++, 0, kind, label, nWay);
    else if (false)
        addSimpleGraphEdge(curSubGraph(), from->id, to->subGraphId, from->outputCount++, 0, kind, label, nWay);
    else
        addComplexGraphEdge(graph, from->subGraphId, to->subGraphId, from->id, to->id, from->outputCount++, kind, label);
}

void LogicalGraphCreator::createLogicalGraph(HqlExprArray & exprs)
{
    graph.setown(createPTree("graph"));

//  beginSubGraph(NULL, false);
    ForEachItemIn(i, exprs)
        createRootGraphActivity(&exprs.item(i));
//  endSubGraph();

    Owned<IWUGraph> wug = wu->updateGraph("Logical");
    wug->setXGMMLTree(graph.getClear());
    wug->setType(GraphTypeEcl);
}


void LogicalGraphCreator::createRootGraphActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_comma:
    case no_compound:
    case no_sortlist:
        {
            ForEachChild(i, expr)
                createRootGraphActivity(expr->queryChild(i));
            return;
        }
    default:
        beginSubGraph(NULL, false);
        createGraphActivity(expr);
        endSubGraph(false);
    }
}

static void expandUnnamedFunnel(HqlExprArray & inputs, IHqlExpression * expr)
{
    while ((expr->getOperator() == no_addfiles) && (expr->queryBody() == expr))
    {
        expandUnnamedFunnel(inputs, expr->queryChild(0));
        expr = expr->queryChild(1);
    }
    inputs.append(*LINK(expr));
}


void LogicalGraphCreator::createGraphActivity(IHqlExpression * expr)
{
    LogicalGraphInfo * extra = queryExtra(expr);
    if (extra->globalId && !inSubQuery())
    {
        extra->id = extra->globalId;
        return;
    }

    //First generate children...
    //MORE: may want to do inputs first and dependents afterwards.
    IAtom * dependencyKind = dependencyAtom;
    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    node_operator op = expr->getOperator();
    HqlExprArray inputs, dependents;
    bool defaultInputs = true;
    switch (op)
    {
    case no_setresult:
    case no_map:
        last = first;
        dependencyKind = NULL;
        break;
    case no_select:
        if (!isNewSelector(expr))
        {
            last = first;
        }
        break;
    case no_addfiles:
        expandUnnamedFunnel(inputs, expr->queryBody());
        defaultInputs = false;
        break;
    case no_colon:
        {
            if (!isWorkflowExpanded(expr))
                defaultInputs = false;

            gatherWorkflowActivities(expr, dependents);
            inputs.append(*LINK(expr->queryChild(0)));
            defaultInputs = false;
            break;
        }
    case no_forcelocal:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
        {
            IHqlExpression * child = expr->queryChild(0);
            createSubGraphActivity(child);
            addDependent(dependents, child);
            defaultInputs = false;
            break;
        }
    }
    if (defaultInputs)
    {
        ForEachChild(i, expr)
        {
            IHqlExpression * cur = expr->queryChild(i);
            if ((i >= first && i < last) && !cur->isAttribute())
                inputs.append(*LINK(cur));
            else if (includeChildInDependents(expr, i))
                gatherGraphActivities(cur, dependents);
        }
    }

    ForEachItemIn(i0, inputs)
        createGraphActivity(&inputs.item(i0));

    //Generate the node for this activity
    unique_id_t id = ++seq;
    bool isGrouped = isGroupedActivity(expr);
    bool isLocal = !isGrouped && isLocalActivity(expr) && localChangesActivity(expr);

    StringBuffer tempText;
    beginActivity(getActivityText(expr, tempText), id);
    StringBuffer eclText;
    toECL(expr->queryBody(), eclText, false, true);
    addAttribute("ecl", eclText.str());
    addAttributeBool("grouped", isGrouped);
    addAttributeBool("local", isLocal);
    IHqlExpression * symbol = queryNamedSymbol(expr);
    if (symbol)
    {
        addAttribute("name", symbol->queryId()->str());
        addAttribute("module", symbol->queryFullContainerId()->str());
        addAttributeInt("line", symbol->getStartLine());
        addAttributeInt("column", symbol->getStartColumn());
    }

    endActivity();

    if (!inSubQuery())
        extra->globalId = id;
    extra->id = id;
    extra->subGraphId = subGraphId;

    ForEachItemIn(i1, inputs)
    {
        IHqlExpression * cur = &inputs.item(i1);
        if (!cur->isAttribute())
        {
            const char * label = NULL;
            switch (expr->getOperator())
            {
            case no_if:
                label = (i1 == 0) ? "True" : "False";
                break;
            case no_fetch:
                break;
            default:
                switch (getChildDatasetType(expr))
                {
                case childdataset_leftright:
                case childdataset_top_left_right:
                    if (!isKeyedJoin(expr))
                    {
                        label = (i1 == 0) ? "LEFT" : "RIGHT";
                    }
                    break;
                }
            }
            connectActivities(cur, expr, NULL, label);
        }
    }

    ForEachItemIn(j, dependents)
        connectActivities(&dependents.item(j), expr, dependencyKind);
}


void LogicalGraphCreator::createSubGraphActivity(IHqlExpression * expr)
{
    beginSubGraph(NULL, true);
    createGraphActivity(expr);
    endSubGraph(true);
}

IPropertyTree * LogicalGraphCreator::curSubGraph()
{
    if (subGraphs.ordinality())
        return &subGraphs.tos();
    return graph;
}

void LogicalGraphCreator::endActivity()
{
    activityNode.clear();
}

void LogicalGraphCreator::endSubGraph(bool nested)
{
    subGraphs.pop();
    subGraphId = savedGraphId.pop();
    if (!nested)
        restoreSubGraphs();
}

static bool exprIsGlobal(IHqlExpression * expr)
{
    HqlExprCopyArray inScope;
    expr->gatherTablesUsed(NULL, &inScope);
    return inScope.ordinality() == 0;
}

void LogicalGraphCreator::gatherWorkflowActivities(IHqlExpression * expr, HqlExprArray & dependents)
{
    HqlExprArray actions;
    expr->queryChild(1)->unwindList(actions, no_comma);
    ForEachItemIn(i, actions)
    {
        IHqlExpression & cur = actions.item(i);
        switch (cur.getOperator())
        {
        case no_success:
        case no_failure:
        case no_recovery:
            {
                IHqlExpression * action = cur.queryChild(0);
                createRootGraphActivity(action);
                OwnedHqlExpr text = createConstant(getOpString(cur.getOperator()));
                OwnedHqlExpr labeled = createComma(LINK(action), LINK(text));
                addDependent(dependents, labeled);
                break;
            }
        }
    }
}

bool LogicalGraphCreator::gatherGraphActivities(IHqlExpression * expr, HqlExprArray & dependents)
{
    LogicalGraphInfo * extra = queryExtra(expr);
    if (extra->noDependents)
        return false;

    //if no_select with new then create a child graph
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_select:
        {
            if (isNewSelector(expr))
            {
                if (exprIsGlobal(expr))
                    createRootGraphActivity(expr);
                else
                    createSubGraphActivity(expr);
                addDependent(dependents, expr);
                return true;
            }
            else
                return false;
        }
    case NO_AGGREGATE:
        {
            if (exprIsGlobal(expr))
                createRootGraphActivity(expr);
            else
                createSubGraphActivity(expr);
            addDependent(dependents, expr);
            return true;
        }
    case no_colon:
        {
            if (!isWorkflowExpanded(expr))
                return false;
            gatherWorkflowActivities(expr, dependents);
        }
        break;
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
    case no_setmeta:
        return false;
    }

    if (extra->globalId && !inSubQuery())
    {
        extra->id = extra->globalId;
        addDependent(dependents, expr);
        return true;
    }

    switch (op)
    {
    case no_newkeyindex:
    case no_table:
        return false;
    }
    bool hasDependents = false;
    ForEachChild(i, expr)
    {
        if (gatherGraphActivities(expr->queryChild(i), dependents))
            hasDependents = true;
    }
    extra->noDependents = !hasDependents;
    return hasDependents;
}

static void capitaliseOpText(StringBuffer & out, IHqlExpression * expr)
{
    const char * opText = getOpString(expr->getOperator());
    //Capitalise the keywords
    if (opText && *opText)
    {
        out.append(*opText);
        while (*++opText)
            out.append((char)tolower(*opText));
    }
}

const char * LogicalGraphCreator::getActivityText(IHqlExpression * expr, StringBuffer & temp)
{
    if (expr->queryBody() != expr)
    {
        IIdAtom * module = includeModuleInText ? expr->queryFullContainerId() : NULL;
        IIdAtom * name = includeNameInText ? expr->queryId() : NULL;
        StringBuffer header;
        if (module)
        {
            if (name)
            {
                //module and name supplied.  module may be the form <module>.<attr>.  
                //If so, display <module>.<attr> if the name matches the attr, else <module>.<attr>::<name>
                const char * dot = strrchr(module->str(), '.');
                if (dot)
                {
                    if (stricmp(dot+1, name->str()) == 0)
                        header.append(module->str());
                    else
                        header.append(module->str()).append("::").append(name->str());
                }
                else
                    header.append(module->str()).append(".").append(name->str());
            }
            else
                header.append(module->str());
        }
        else if (name)
            header.append(name->str());

        if (header.length())
            temp.append(header).append("\n");
    }
    if (displayJavadoc)
    {
        Owned<IPropertyTree> doc = expr->getDocumentation();
        if (doc)
        {
            doc->getProp("", temp);
            if (displayJavadocParameters && doc->hasProp("param"))
            {
                temp.append("\nParameters:");
                Owned<IPropertyTreeIterator> iter = doc->getElements("param");
                ForEach(*iter)
                {
                    Owned<IPropertyTree> cur = &iter->get();
                    temp.append("\n");
                    cur->getProp("", temp);
                }
            }
            return temp.str();
        }
    }
    switch (expr->getOperator())
    {
    case no_table:
        {
            IHqlExpression * mode = expr->queryChild(2);
            switch (mode->getOperator())
            {
            case no_csv:
            case no_xml:
                temp.append(getOpString(expr->getOperator())).append(" ");
                break;
            }
            temp.append("Dataset");
            queryExpandFilename(temp, expr->queryChild(0));
            break;
        }
        break;
    case no_newkeyindex:
        {
            temp.append(getOpString(expr->getOperator()));
            queryExpandFilename(temp, expr->queryChild(3));
            break;
        }
    case no_newusertable:
        {
            temp.append("Table");
            break;
        }
    case no_output:
        {
            IHqlExpression * filename = queryRealChild(expr, 1);
            if (filename)
            {
                temp.append("Output");
                if (expr->hasAttribute(xmlAtom))
                    temp.append(" XML");
                else if (expr->hasAttribute(csvAtom))
                    temp.append(" CSV");
                queryExpandFilename(temp, filename);
            }
            else
            {
                IHqlExpression * seq = querySequence(expr);
                IHqlExpression * name = queryResultName(expr);
                temp.append(::getActivityText(TAKworkunitwrite)).append("\n");
                getStoredDescription(temp, seq, name, true);
            }
            break;
        }
    case no_temptable:
    case no_inlinetable:
        temp.append("Inline Dataset");
        break;
    case no_addfiles:
        temp.append(::getActivityText(TAKfunnel));
        break;
    case no_colon:
        {
            temp.append("Workflow:");
            HqlExprArray actions;
            expr->queryChild(1)->unwindList(actions, no_comma);
            ForEachItemIn(i, actions)
            {
                IHqlExpression * cur = &actions.item(i);
                if (!cur->isAttribute())
                {
                    temp.append(' ');
                    toECL(cur, temp, false, false);
                }
            }
            break;
        }
    case no_extractresult:
    case no_setresult:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            temp.append("Store\n");
            getStoredDescription(temp, sequence, name, true);
            break;
        }
    case no_join:
        if (isKeyedJoin(expr))
        {
            temp.append("Keyed Join");
            IHqlExpression * index = expr->queryChild(1)->queryNormalizedSelector();
            if (index->getOperator() == no_newkeyindex)
                queryExpandFilename(temp, index->queryChild(3));
        }
        else
            temp.append("Join");
        break;
    case no_selfjoin:
        temp.append("Self Join");
        break;
    case no_select:
        temp.append("Select ");
        if (expr->isDataset())
            temp.append("Dataset");
        else if (expr->isDatarow())
            temp.append("Row");
        else
            temp.append("Field");
        temp.append("\n");
        temp.append(expr->queryChild(1)->queryName());
        break;
    case no_group:
        {
            IHqlExpression * sortlist = queryRealChild(expr, 1);
            if (sortlist && sortlist->numChildren() != 0)
                capitaliseOpText(temp, expr);
            else
                temp.append("Degroup");
            break;
        }
    default:
        if (expr->isAction() || expr->isDataset() || expr->isAggregate())
            capitaliseOpText(temp, expr);
        else
            temp.append("Result");
        break;
    }
    return temp.str();
}

bool LogicalGraphCreator::inSubQuery() const
{
    return subGraphs.ordinality() > 1;
}

bool LogicalGraphCreator::isWorkflowExpanded(IHqlExpression * expr) const
{
    IHqlExpression * actions = expr->queryChild(1);
    ForEachChild(i, actions)
    {
        IHqlExpression * cur = actions->queryChild(i);
        switch (cur->getOperator())
        {
        case no_persist:
            if (!expandPersist)
                return false;
            break;
        case no_stored:
            if (!expandStored)
                return false;
            break;
        default:
            return true;
        }
    }
    return true;
}

LogicalGraphInfo * LogicalGraphCreator::queryExtra(IHqlExpression * expr)
{
    LogicalGraphInfo * extra = (LogicalGraphInfo *)expr->queryTransformExtra();
    if (!extra)
    {
        extra = new LogicalGraphInfo(expr);
        expr->setTransformExtraOwned(extra);
    }
    return extra;
}


void LogicalGraphCreator::restoreSubGraphs()
{
    subGraphs.kill();
    unsigned level = savedLevels.pop();
    while (saved.ordinality() != level)
        subGraphs.append(saved.popGet());
}

void LogicalGraphCreator::saveSubGraphs()
{
    savedLevels.append(saved.ordinality());
    ForEachItemInRev(i, subGraphs)
        saved.append(subGraphs.item(i));
    subGraphs.kill(true);
}
