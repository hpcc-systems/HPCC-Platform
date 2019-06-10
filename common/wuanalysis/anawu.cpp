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

#include "jliball.hpp"

#include "workunit.hpp"
#include "anacommon.hpp"
#include "anarule.hpp"
#include "anawu.hpp"

class WuScope;
class WorkunitAnalyser;

class WuScopeHashTable : public SuperHashTableOf<WuScope, const char>
{
public:
    ~WuScopeHashTable() { _releaseAll(); }

    void addNew(WuScope * newWuScope) { SuperHashTableOf::addNew(newWuScope);}
    virtual void     onAdd(void *et) {};
    virtual void     onRemove(void *et);
    virtual unsigned getHashFromElement(const void *et) const;
    virtual unsigned getHashFromFindParam(const void *fp) const;
    virtual const void * getFindParam(const void *et) const;
    virtual bool matchesFindParam(const void *et, const void *key, unsigned fphash) const;
    virtual bool matchesElement(const void *et, const void *searchET) const;
};

inline unsigned hashScope(const char * name) { return hashc((const byte *)name, strlen(name), 0); }

class WuScope : public CInterface, implements IWuEdge, implements IWuActivity
{
public:
    WuScope(const char * _name, WuScope * _parent) : name(_name), parent(_parent)
    {
        attrs.setown(createPTree());
    }

    void applyRules(WorkunitAnalyser & analyser);
    void connectActivities();

    WuScope * select(const char * scope);  // Returns matching wuScope (create if no pre-existing)
    void setInput(unsigned i, WuScope * scope);  // Save i'th target in inputs
    void setOutput(unsigned i, WuScope * scope); // Save i'th source in output

    virtual stat_type getStatRaw(StatisticKind kind, StatisticKind variant = StKindNone) const;
    virtual unsigned getAttr(WuAttr kind) const;
    virtual void getAttr(StringBuffer & result, WuAttr kind) const;

    inline const char * queryName() const { return name; }
    inline IPropertyTree * queryAttrs() const { return attrs; }
    virtual WuScope * querySource() override;
    virtual WuScope * queryTarget() override;
    virtual IWuEdge * queryInput(unsigned idx)  { return (idx < inputs.size()) ? inputs[idx]:nullptr; }
    virtual IWuEdge * queryOutput(unsigned idx) { return (idx < outputs.size()) ? outputs[idx]:nullptr; }
    StatisticScopeType queryScopeType() const   { return (StatisticScopeType)attrs->getPropInt("@stype");}
    const char * queryScopeTypeName() const     { return ::queryScopeTypeName(queryScopeType()); }

    void trace(unsigned indent=0) const;
protected:
    StringAttr name;
    WuScope * parent = nullptr;
    Owned<IPropertyTree> attrs;
    WuScopeHashTable scopes;
    std::vector<WuScope *> inputs;
    std::vector<WuScope *> outputs;
};

//-----------------------------------------------------------------------------------------------------------
class WorkunitAnalyser
{
public:
    WorkunitAnalyser(WuAnalyseOptions & _options);

    void check(const char * scope, IWuActivity & activity);
    void analyse(IConstWorkUnit * wu);

protected:
    void collateWorkunitStats(IConstWorkUnit * workunit, const WuScopeFilter & filter);
    void printWarnings();
    WuScope * selectFullScope(const char * scope);

protected:
    CIArrayOf<AActivityRule> rules;
    CIArrayOf<PerformanceIssue> issues;
    WuScope root;
    WuAnalyseOptions & options;
};

//-----------------------------------------------------------------------------------------------------------
void WuScopeHashTable::onRemove(void *et)
{
    WuScope * elem = reinterpret_cast<WuScope *>(et);
    elem->Release();
}
unsigned WuScopeHashTable::getHashFromElement(const void *et) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    return hashScope(elem->queryName());
}

unsigned WuScopeHashTable::getHashFromFindParam(const void *fp) const
{
    const char * search = reinterpret_cast<const char *>(fp);
    return hashScope(search);
}

const void * WuScopeHashTable::getFindParam(const void *et) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    return elem->queryName();
}

bool WuScopeHashTable::matchesFindParam(const void *et, const void *key, unsigned fphash) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    const char * search = reinterpret_cast<const char *>(key);
    return streq(elem->queryName(), search);
}

bool WuScopeHashTable::matchesElement(const void *et, const void *searchET) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    const WuScope * searchElem = reinterpret_cast<const WuScope *>(searchET);
    return streq(elem->queryName(), searchElem->queryName());
}

//-----------------------------------------------------------------------------------------------------------
void WuScope::applyRules(WorkunitAnalyser & analyser)
{
    for (auto & cur : scopes)
    {
        if (cur.queryScopeType() == SSTactivity)
            analyser.check(cur.queryName(), cur);
        cur.applyRules(analyser);
    }
}

void WuScope::connectActivities()
{
    //Yuk - scopes can be added to while they are being iterated, so need to create a list first
    CICopyArrayOf<WuScope> toWalk;
    for (auto & cur : scopes)
        toWalk.append(cur);

    ForEachItemIn(i, toWalk)
    {
        WuScope & cur = toWalk.item(i);

        if (cur.queryScopeType() == SSTedge)
        {
            WuScope * source = cur.querySource();
            WuScope * sink = cur.queryTarget();
            if (source)
                source->setOutput(cur.getAttr(WaSourceIndex), &cur);
            if (sink)
                sink->setInput(cur.getAttr(WaTargetIndex), &cur);
        }
        cur.connectActivities();
    }
}

WuScope * WuScope::select(const char * scope)
{
    assertex(scope);
    WuScope * match = scopes.find(scope);
    if (match)
        return match;
    match = new WuScope(scope, this);
    scopes.addNew(match);
    return match;
}

void WuScope::setInput(unsigned i, WuScope * scope)
{
    while (inputs.size() <= i)
        inputs.push_back(nullptr);
    inputs[i] = scope;
}

void WuScope::setOutput(unsigned i, WuScope * scope)
{
    while (outputs.size() <= i)
        outputs.push_back(nullptr);
    outputs[i] = scope;
}

WuScope * WuScope::querySource()
{
    const char * source = attrs->queryProp("@IdSource");
    if (!source)
        return nullptr;
    return parent->select(source);
}

WuScope * WuScope::queryTarget()
{
    const char * target = attrs->queryProp("@IdTarget");
    if (!target)
        return nullptr;
    return parent->select(target);
}

stat_type WuScope::getStatRaw(StatisticKind kind, StatisticKind variant) const
{
    StringBuffer name;
    name.append('@').append(queryStatisticName(kind | variant));
    return attrs->getPropInt64(name);
}

unsigned WuScope::getAttr(WuAttr attr) const
{
    StringBuffer name;
    name.append('@').append(queryWuAttributeName(attr));
    return attrs->getPropInt64(name);
}

void WuScope::getAttr(StringBuffer & result, WuAttr attr) const
{
    StringBuffer name;
    name.append('@').append(queryWuAttributeName(attr));
    attrs->getProp(result, name);
}

void WuScope::trace(unsigned indent) const
{
    printf("%*s%s: \"%s\" (", indent, " ", queryScopeTypeName(), queryName());
    for (auto in : inputs)
        printf("%s ", in->queryName());
    printf("->");
    for (auto out : outputs)
        printf(" %s",  out->queryName());
    printf(") [");
    printf("children: ");
    for(auto & scope: scopes)
    {
        printf("%s ", scope.queryName());
    }
    printf("] {\n");

    printXML(queryAttrs(), indent);
    for (auto & cur : scopes)
    {
        cur.trace(indent+4);
    }
    printf("%*s}\n",indent, " ");
}


//-----------------------------------------------------------------------------------------------------------
/* Callback used to output scope properties as xml */
class StatsGatherer : public IWuScopeVisitor
{
public:
    StatsGatherer(IPropertyTree * _scope) : scope(_scope){}

    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & cur) override
    {
        StringBuffer name;
        name.append('@').append(queryStatisticName(kind));
        scope->setPropInt64(name, value);
    }
    virtual void noteAttribute(WuAttr attr, const char * value)
    {
        StringBuffer name;
        name.append('@').append(queryWuAttributeName(attr));
        scope->setProp(name, value);
    }
    virtual void noteHint(const char * kind, const char * value)
    {
        throwUnexpected();
    }
    IPropertyTree * scope;
};


//-----------------------------------------------------------------------------------------------------------
WorkunitAnalyser::WorkunitAnalyser(WuAnalyseOptions & _options) : root("", nullptr), options(_options)
{
    gatherRules(rules);
}

void WorkunitAnalyser::check(const char * scope, IWuActivity & activity)
{
    if (activity.getStatRaw(StTimeLocalExecute, StMaxX) < options.minInterestingTime)
        return;

    Owned<PerformanceIssue> highestCostIssue;
    ForEachItemIn(i, rules)
    {
        if (rules.item(i).isCandidate(activity))
        {
            Owned<PerformanceIssue> issue (new PerformanceIssue);
            if (rules.item(i).check(*issue, activity, options))
            {
                if (issue->getCost() >= options.minCost)
                {
                    if (!highestCostIssue || highestCostIssue->getCost() < issue->getCost())
                        highestCostIssue.setown(issue.getClear());
                }
            }
        }
    }
    if (highestCostIssue)
    {
        highestCostIssue->setScope(scope);
        issues.append(*highestCostIssue.getClear());
    }
}

void WorkunitAnalyser::analyse(IConstWorkUnit * wu)
{
    WuScopeFilter filter;
    filter.addOutputProperties(PTstatistics).addOutputProperties(PTattributes);
    filter.finishedFilter();
    collateWorkunitStats(wu, filter);
    root.connectActivities();
    // root.trace();
    root.applyRules(*this);
    printWarnings();
}

void WorkunitAnalyser::collateWorkunitStats(IConstWorkUnit * workunit, const WuScopeFilter & filter)
{
    Owned<IConstWUScopeIterator> iter = &workunit->getScopeIterator(filter);
    ForEach(*iter)
    {
        try
        {
            WuScope * scope = selectFullScope(iter->queryScope());

            StatsGatherer callback(scope->queryAttrs());
            scope->queryAttrs()->setPropInt("@stype", iter->getScopeType());
            iter->playProperties(callback);
        }
        catch (IException * e)
        {
            e->Release();
        }
    }
}

void WorkunitAnalyser::printWarnings()
{
    issues.sort(compareIssuesCostOrder);

    ForEachItemIn(i, issues)
        issues.item(i).print();
}

WuScope * WorkunitAnalyser::selectFullScope(const char * scope)
{
    StringBuffer temp;
    WuScope * resolved = &root;
    for (;;)
    {
        if (!*scope)
            return resolved;
        const char * dot = strchr(scope, ':');
        if (!dot)
            return resolved->select(scope);

        temp.clear().append(dot-scope, scope);
        resolved = resolved->select(temp.str());
        scope = dot+1;
    }
}

//---------------------------------------------------------------------------------------------------------------------

void analyseWorkunit(IConstWorkUnit * wu)
{
    WuAnalyseOptions options;
    options.skewThreshold = statSkewPercent(10);
    WorkunitAnalyser analyser(options);
    analyser.analyse(wu);
}
