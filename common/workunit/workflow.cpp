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

#include "jlib.hpp"
#include "workunit.hpp"
#include "jptree.hpp"
#include "jlog.hpp"
#include "jregexp.hpp"
#include "workflow.hpp"

//------------------------------------------------------------------------------------------
/* Parallel Workflow explanation
Prerequisite
Being able to understand how the workflow engine works requires understanding the code generator. In particular, the way that queries get translated into a workflow structure.
Many details are important or unexpected. For example, persist items come as a pair, but in the opposite order to what you might expect.
There is a useful description in the WorkUnits.rst devdoc.

Key information
* All items are executed a maximum of one time per query. (Unless they are recovered)
* ECL actions are in one-to-one correspondence with the workflow items that houses the action.

Ready criteria
The core criteria defining when items are added to the task queue is if they have any unperformed dependencies. A second criteria checks that the item is active.
An item, that has been popped from the task queue, should be executed is if it is both active and alive.

General process
* The program checks workflow items against a list of exceptions to determine if parallel execution is supported.
* The program recursively traces through each item’s dependencies, constructing the “graph of successors”. See below
* Items without dependencies are placed on the starting task queue
* Threads are created
* Threads perform the thread specific process
* Threads finish, once all items have been executed

Thread specific process
* Wait for an item to be added to the task queue
* Pop an item from the task queue
* Execute item
* Alert successors to the item (convey that one of their dependencies has been completed)
* Add any successors to the task queue if they meet the "ready criteria"

What is the graph of successors?
The relationship between an item and its dependency is two-directional.
* The dependency must be done first
* The item should be done second. Therefore, the item is a successor to the dependency.
The term graph represents the idea that the successor of an item may also have its own successors. (You could sketch this out visually)
This is a consequence of allowing an item's dependency to also have its own dependencies.
An ECL query always generates at least one item with zero successors, called the "parent item”. This is the item to execute last (unless the query fails)

Dependent successors are those described above; they are the reverse of a dependency.
Logical successors are a second type of successor. Logical successors have no dependency on their predecessor, yet must execute afterwards.
Logical successors are used for a variety of reasons involving ORDERED, SEQUENTIAL, IF, SUCCESS/FAILURE.

There are scenarios where an item will execute before its logical predecessor. (But this can't happen for a dependent successor)
i.e. the ECL code: PARALLEL(b, ORDERED(a,b))
This may cause action b to be executed before a - even though there is a logical successorship from a to b due to ORDERED.
You could say that this logical successorship is made obsolete by the encompassing PARALLEL statement.
This code example shows that although logical successorships are added in the graph of successors, they may never be used.

PARALLEL, ORDERED and SEQUENTIAL are ways to group actions and to specify if they have any ordering requirements. (An ordering requirement could be: action 1 needs to be performed before action 2)
I will describe how they work in terms of the first and second actions in the actionlist, without any loss of generality.
The relationship from the second action to the first is *exactly* the same as the relationship from the third action to the second, and so on.

PARALLEL
The actions in a parallel actionlist have no special ordering requirements. Any actions or their dependencies can be performed concurrently.
In relation to the workflow engine, there are no logical dependencies between the actions or their dependencies

SEQUENTIAL
The actions in a sequential actionlist have the most constrained ordering requirements. Firstly, the actions must be performed in order. Secondly, any dependencies to an action can only be started once the previous action has finished.
In relation to the workflow engine, the second action in the actionlist has a logical dependency on the first action. Furthermore, each of the second action's dependencies has a logical dependency on the first action.

ORDERED
The actions in an ordered actionlist have a less constrained ordering requirement than sequential. Only the actions in the actionlist must be performed in order, but there is no special ordering requirement for their dependencies.
In relation to the workflow engine, the second action in the actionlist has a logical dependency on the first action. This is not true of the second action's dependencies, which can be executed at any point.

Active
Any actions explicitly called in the ECL query are active. For the query to finish, it is always necessary for these item to execute.
Any items that these actions depend on also get marked as active.
Items that start without being active (i.e. those that might not execute during the workunit) are:
* items that are logical successors of other items, and not activated by another route
These could be:
* items that are contingencies (SUCCESS/FAILURE)
* items that are the trueresult or falseresult of a conditional IF function
If one of these items is going to execute, then they are marked as active.
For example, logical successorships (described above) entail a predecessor "activating" its successor.
An item is active when it has a reason to execute.
Anything that uses activate()/deactivate() is protected by a critical section.

Alive
When an item starts executing, it has to be "alive". This means that it fulfills the following condition: (!abort || item.queryException() || item.queryContingencyWithin())
If the item is no longer alive, then instead of processing it, the item is discarded.
Whenever the global abort is false, all items are alive.
If the global abort is true, then only items that are part of a contingency or have an exception are alive. (For an item to have an exception, either it has failed or its children have failed)
Items that are part of a contingency **and** have an exception are treated like items with just an exception.

The global abort changing is the only reason that items can go from alive to dead.
Items are made inactive once they turn dead, so that a different logical predecessor can re-activate them.

Complications
The algorithm is made more complicated by having to
* co-ordinate when the worker threads stop
* catch any failed workflow items and perform the failure contingencies
* identify whether persist items (and dependencies) are up-to-date
* protect shared resources such as the task queue from race conditions.
*/

EnumMapping wftypes[] =
{
    { WFTypeNormal, "normal" },
    { WFTypeSuccess, "success" },
    { WFTypeFailure, "failure" },
    { WFTypeRecovery, "recovery" },
    { WFTypeWait, "wait" },
    { WFTypeSize, NULL }
};

EnumMapping wfmodes[] =
{
    { WFModeNormal, "normal" },
    { WFModeCondition, "condition" },
    { WFModeSequential, "sequential" },
    { WFModeParallel, "parallel" },
    { WFModePersist, "persist" },
    { WFModeBeginWait, "bwait" },
    { WFModeWait, "wait" },
    { WFModeOnce, "once" },
    { WFModeCritical, "critical" },
    { WFModeOrdered, "ordered" },
    { WFModeConditionExpression, "condition expression" },
    { WFModeSize, NULL}
};

EnumMapping wfstates[] =
{
   { WFStateNull, "null" },
   { WFStateReqd, "reqd" },
   { WFStateDone, "done" },
   { WFStateFail, "fail" },
   { WFStateSkip, "skip" },
   { WFStateWait, "wait" },
   { WFStateBlocked, "block" },
   { WFStateSize, NULL }
};

static void setEnum(IPropertyTree *p, const char *propname, int value, EnumMapping *map)
{
    const char * mapped = getEnumText(value, map, nullptr);
    if (!mapped)
        assertex(!"Unexpected value in setEnum");
    p->setProp(propname, mapped);
}

static int getEnum(IPropertyTree *p, const char *propname, EnumMapping *map)
{
    const char *v = p->queryProp(propname);
    if (v)
        return getEnum(v, map);
    return 0;
}

const char * queryWorkflowTypeText(WFType type)
{
    return getEnumText(type, wftypes);
}

const char * queryWorkflowModeText(WFMode mode)
{
    return getEnumText(mode, wfmodes);
}

const char * queryWorkflowStateText(WFState state)
{
    return getEnumText(state, wfstates);
}


class CWorkflowDependencyIterator : implements IWorkflowDependencyIterator, public CInterface
{
public:
    CWorkflowDependencyIterator(IPropertyTree * tree) { iter.setown(tree->getElements("Dependency")); }
    IMPLEMENT_IINTERFACE;
    bool                first() { return iter->first(); }
    bool                isValid() { return iter->isValid(); }
    bool                next() { return iter->next(); }
    unsigned            query() const { return iter->query().getPropInt("@wfid"); }
private:
    Owned<IPropertyTreeIterator> iter;
};

class CWorkflowEvent : public CInterface, implements IWorkflowEvent
{
public:
    CWorkflowEvent(char const * _name, char const * _text) : name(_name), text(_text) {}
    IMPLEMENT_IINTERFACE;
    virtual char const * queryName() const { return name.get(); }
    virtual char const * queryText() const { return text.get(); }
    virtual bool matches(char const * trialName, char const * trialText) const { return((strcmp(trialName, name.get()) == 0) && WildMatch(trialText, text.get(), true)); }
private:
    StringAttr name;
    StringAttr text;
};

class CWorkflowItem : implements IWorkflowItem, public CInterface
{
public:
    CWorkflowItem(IPropertyTree & _tree) { tree.setown(&_tree); }
    CWorkflowItem(IPropertyTree * ptree, unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor)
    {
        tree.setown(LINK(ptree->addPropTree("Item")));
        tree->setPropInt("@wfid", wfid);
        setEnum(tree, "@type", type, wftypes);
        setEnum(tree, "@mode", mode, wfmodes);
        if(success) tree->setPropInt("@success", success);
        if(failure) tree->setPropInt("@failure", failure);
        if(recovery && retriesAllowed)
        {
            tree->setPropInt("@recovery", recovery);
            tree->setPropInt("@retriesAllowed", retriesAllowed);
            tree->addPropTree("Dependency")->setPropInt("@wfid", recovery);
        }
        if(contingencyFor) tree->setPropInt("@contingencyFor", contingencyFor);
        reset();
    }

    IMPLEMENT_IINTERFACE;
    //info set at compile time
    virtual unsigned     queryWfid() const { return tree->getPropInt("@wfid"); }
    virtual bool         isScheduled() const { return tree->hasProp("Schedule"); }
    virtual bool         isScheduledNow() const { return (tree->hasProp("Schedule") && !tree->hasProp("Schedule/Event")); }
    virtual IWorkflowEvent * getScheduleEvent() const { if(tree->hasProp("Schedule/Event")) return new CWorkflowEvent(tree->queryProp("Schedule/Event/@name"), tree->queryProp("Schedule/Event/@text")); else return NULL; }
    virtual unsigned     querySchedulePriority() const { return (tree->hasProp("Schedule") ? tree->getPropInt("Schedule/@priority", 0) : 0); }
    virtual bool         hasScheduleCount() const { return tree->hasProp("Schedule/@count"); }
    virtual unsigned     queryScheduleCount() const { assertex(tree->hasProp("Schedule/@count")); return tree->getPropInt("Schedule/@count"); }
    virtual IWorkflowDependencyIterator * getDependencies() const { return new CWorkflowDependencyIterator(tree); }
    virtual WFType       queryType() const { return static_cast<WFType>(getEnum(tree, "@type", wftypes)); }
    virtual IStringVal & getLabel(IStringVal & val) const { val.set(tree->queryProp("@label")); return val; }
    virtual WFMode       queryMode() const { return static_cast<WFMode>(getEnum(tree, "@mode", wfmodes)); }
    virtual unsigned     querySuccess() const { return tree->getPropInt("@success", 0); }
    virtual unsigned     queryFailure() const { return tree->getPropInt("@failure", 0); }
    virtual unsigned     queryRecovery() const { return tree->getPropInt("@recovery", 0); }
    virtual unsigned     queryRetriesAllowed() const { return tree->getPropInt("@retriesAllowed", 0); }
    virtual unsigned     queryContingencyFor() const { return tree->getPropInt("@contingencyFor", 0); }
    virtual IStringVal & getPersistName(IStringVal & val) const { val.set(tree->queryProp("@persistName")); return val; }
    virtual unsigned     queryPersistWfid() const { return tree->getPropInt("@persistWfid", 0); }
    virtual int          queryPersistCopies() const { return tree->getPropInt("@persistCopies", 0); }
    virtual bool         queryPersistRefresh() const { return tree->getPropBool("@persistRefresh", true); }
    virtual IStringVal & getCriticalName(IStringVal & val) const { val.set(tree->queryProp("@criticalName")); return val; }
    virtual IStringVal & queryCluster(IStringVal & val) const { val.set(tree->queryProp("@cluster")); return val; }
    virtual void         setScheduledNow() { tree->setPropTree("Schedule"); setEnum(tree, "@state", WFStateReqd, wfstates); }
    virtual void         setScheduledOn(char const * name, char const * text) { IPropertyTree * stree =  tree->setPropTree("Schedule")->setPropTree("Event"); stree->setProp("@name", name); stree->setProp("@text", text);; setEnum(tree, "@state", WFStateWait, wfstates); }
    virtual void         setSchedulePriority(unsigned priority) { assertex(tree->hasProp("Schedule")); tree->setPropInt("Schedule/@priority", priority); }
    virtual void         setScheduleCount(unsigned count) { assertex(tree->hasProp("Schedule")); tree->setPropInt("Schedule/@count", count); tree->setPropInt("Schedule/@countRemaining", count); }
    virtual void         addDependency(unsigned wfid) { tree->addPropTree("Dependency")->setPropInt("@wfid", wfid); }
    virtual void         setPersistInfo(char const * name, unsigned wfid, int numPersistInstances, bool refresh)
    {
        tree->setProp("@persistName", name);
        tree->setPropInt("@persistWfid", wfid);
        if (numPersistInstances != 0)
            tree->setPropInt("@persistCopies", (int)numPersistInstances);
        tree->setPropBool("@persistRefresh", refresh);
    }
    virtual void         setCriticalInfo(char const * name) { tree->setProp("@criticalName", name);}
    virtual void         setCluster(const char * cluster) { tree->setProp("@cluster", cluster); }
    //info set at run time
    virtual unsigned     queryScheduleCountRemaining() const { assertex(tree->hasProp("Schedule")); return tree->getPropInt("Schedule/@countRemaining"); }
    virtual WFState      queryState() const { return static_cast<WFState>(getEnum(tree, "@state", wfstates)); }
    virtual unsigned     queryRetriesRemaining() const { return tree->getPropInt("@retriesRemaining"); }
    virtual int          queryFailCode() const { return tree->getPropInt("@failcode"); }
    virtual char const * queryFailMessage() const { return tree->queryProp("@failmsg"); }
    virtual char const * queryEventName() const { return tree->queryProp("@eventname"); }
    virtual char const * queryEventExtra() const { return tree->queryProp("@eventextra"); }
    virtual void         setState(WFState state) { setEnum(tree, "@state", state, wfstates); }
    virtual unsigned     queryScheduledWfid() const { return tree->getPropInt("@swfid", 0); }
    virtual void         setScheduledWfid(unsigned wfid) { tree->setPropInt("@swfid", wfid); }
    virtual void         setLabel(const char * label) { tree->setProp("@label", label); }
    virtual bool         testAndDecRetries()
    {
        assertex(tree->hasProp("@retriesAllowed"));
        unsigned rem = tree->getPropInt("@retriesRemaining", 0);
        if(rem==0)
            return false;
        tree->setPropInt("@retriesRemaining", rem-1);
        return true;
    }
    virtual bool         decAndTestScheduleCountRemaining()
    {
        if(!tree->hasProp("Schedule/@count"))
            return true;
        unsigned rem = tree->getPropInt("Schedule/@countRemaining");
        assertex(rem>0);
        tree->setPropInt("Schedule/@countRemaining", rem-1);
        return (rem>1);
    }
    virtual void incScheduleCount()
    {
        unsigned rem = tree->getPropInt("Schedule/@countRemaining");
        tree->setPropInt("Schedule/@countRemaining", rem+1);
    }
    virtual void         setFailInfo(int code, char const * message)
    {
        tree->setPropInt("@failcode", code);
        tree->setProp("@failmsg", message);
    }
    virtual void         setEvent(const char * name, const char * extra)
    {
        if (name)
            tree->setProp("@eventname", name);
        if (extra)
            tree->setProp("@eventextra", extra);
    }
    virtual void         reset()
    {
        if(tree->hasProp("@retriesAllowed"))
            tree->setPropInt("@retriesRemaining", tree->getPropInt("@retriesAllowed"));
        if(tree->hasProp("Schedule/@count"))
            tree->setPropInt("Schedule/@countRemaining", tree->getPropInt("Schedule/@count"));
        tree->removeProp("@failcode");
        tree->removeProp("@failmsg");
        tree->removeProp("@eventname");
        tree->removeProp("@eventtext");
        if(isScheduled())
        {
            if(isScheduledNow())
                setState(WFStateReqd);
            else if (hasScheduleCount() && (queryScheduleCountRemaining() == 0))
                setState(WFStateDone);
            else
                setState(WFStateWait);
        }
        else if(queryType() == WFTypeRecovery)
            setState(WFStateSkip);
        else
            setState(WFStateNull);
    }
    virtual void         syncRuntimeData(IConstWorkflowItem const & other)
    {
        WFState state = other.queryState();
        setState(state);
        if(tree->hasProp("@retriesAllowed"))
            tree->setPropInt("@retriesRemaining", other.queryRetriesRemaining());
        if(tree->hasProp("Schedule/@count"))
            tree->setPropInt("Schedule/@countRemaining", other.queryScheduleCountRemaining());
        if(state == WFStateFail)
        {
            tree->setPropInt("@failcode", other.queryFailCode());
            tree->setProp("@failmsg", other.queryFailMessage());
        }
        setEvent(other.queryEventName(), other.queryEventExtra());
    }
private:
    Owned<IPropertyTree> tree;
};

class CCloneWorkflowItem : public CInterface, implements IRuntimeWorkflowItem
{
private:
    class CCloneSchedule : public CInterface
    {
    private:
        bool now;
        unsigned priority;
        bool counting;
        unsigned count;
        unsigned countRemaining;
        Owned<IWorkflowEvent> event;
    public:
        CCloneSchedule(IConstWorkflowItem const * other)
        {
            now = other->isScheduledNow();
            priority = other->querySchedulePriority();
            counting = other->hasScheduleCount();
            if(counting)
            {
                count = other->queryScheduleCount();
                countRemaining = other->queryScheduleCountRemaining();
            }
            else
            {
                count = 0;
                countRemaining = 0;
            }
            event.setown(other->getScheduleEvent());
        }
        bool isNow() const { return now; }
        unsigned queryPriority() const { return priority; }
        bool hasCount() const { return counting; }
        unsigned queryCount() const { return count; }
        unsigned queryCountRemaining() const { return countRemaining; }
        bool decAndTestCountRemaining()
        {
            if(!counting)
                return true;
            if(countRemaining)
                countRemaining--;
            return (countRemaining>0);
        }
        void incCountRemaining()
        {
            if(counting)
                countRemaining++;
        }
        void resetCount() { if(counting) countRemaining = count; }
        IWorkflowEvent * getEvent() const { return event.getLink(); }
    };

    class CCloneIterator : public CInterface, public IWorkflowDependencyIterator
    {
    public:
        CCloneIterator(IntArray const & _array) : array(_array), idx(0) {}
        IMPLEMENT_IINTERFACE;
        virtual bool first() { idx = 0; return isValid(); }
        virtual bool isValid() { return array.isItem(idx); }
        virtual bool next() { idx++; return isValid(); }
        virtual unsigned query() const { return array.item(idx); }
    private:
        IntArray const & array;
        aindex_t idx;
    };

    unsigned wfid;
    //If an item has an exception, only the failure contingency should execute
    Owned<WorkflowException> thisException;
    Owned<CCloneSchedule> schedule;
    IntArray dependencies;
    IntArray dependentSuccessors;
    //These are the items that are activated, upon completion (of this item)
    IntArray logicalSuccessors;
    //This is the number of unperformed dependencies belonging to the item. It is decreased until it reaches 0
    std::atomic<unsigned int> numDependencies{0U};
    //An item will only be executed if it is active
    std::atomic<bool> active{false};
    //The flag is the runtime context in which an item has been added to the task queue. This means that before the execution starts, it isn't known whether each item will be executed as part of a contingency or otherwise.
    //This catches contingency failures
    unsigned withinContingency = 0;
    WFType type = WFTypeNormal;
    WFMode mode = WFModeNormal;
    unsigned success;
    unsigned failure;
    unsigned recovery;
    unsigned retriesAllowed;
    unsigned contingencyFor;
    unsigned scheduledWfid;
    WFState state = WFStateNull;
    unsigned retriesRemaining;
    int failcode;
    StringAttr failmsg;
    SCMStringBuffer persistName;
    SCMStringBuffer clusterName;
    SCMStringBuffer label;
    unsigned persistWfid;
    int persistCopies;
    bool persistRefresh = true;
    SCMStringBuffer criticalName;
    StringAttr eventName;
    StringAttr eventExtra;

public:
    CCloneWorkflowItem(){}
    CCloneWorkflowItem(unsigned _wfid)
    {
        wfid = _wfid;
    }
    IMPLEMENT_IINTERFACE;
    void incNumDependencies()
    {
        numDependencies++;
    }
    unsigned atomicDecNumDependencies()
    {
        return numDependencies.fetch_sub(1);
    }
    unsigned queryNumDependencies() const { return numDependencies; }
    unsigned queryNumDependentSuccessors() const { return dependentSuccessors.ordinality(); }
    unsigned queryNumLogicalSuccessors() const { return logicalSuccessors.ordinality(); }
    bool isDependentSuccessorsEmpty() const
    {
        return dependentSuccessors.empty();
    }
    void addDependentSuccessor(CCloneWorkflowItem * next)
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Workflow item %u has marked workflow item %u as its dependent successor", wfid, next->queryWfid());
#endif
        dependentSuccessors.append(next->queryWfid());
        next->incNumDependencies();
    }
    void addLogicalSuccessor(CCloneWorkflowItem * next)
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Workflow item %u has marked workflow item %u as its logical successor", wfid, next->queryWfid());
#endif
        logicalSuccessors.append(next->queryWfid());
        //note that dependency count is not incremented, since logical successors don't follow as dependents
        //Instead, logical relationships are used to activate the successors
    }
    bool hasLogicalSuccessor(unsigned wfid)
    {
        return (logicalSuccessors.contains(wfid));
    }
    //For condition expression
    void removeLogicalSuccessors()
    {
        if(logicalSuccessors.empty())
            throwUnexpected();
        logicalSuccessors.clear();
    }
    IWorkflowDependencyIterator * getDependentSuccessors() const
    {
        return new CCloneIterator(dependentSuccessors);
    }
    IWorkflowDependencyIterator * getLogicalSuccessors() const
    {
        return new CCloneIterator(logicalSuccessors);
    }
    void activate()
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "workflow item %u [%p] is activated", wfid, this);
#endif
        active = true;
    }
    void deactivate()
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "workflow item %u [%p] is deActivated", wfid, this);
#endif
        active = false;
    }
    bool isActive() const { return active; }
    void setMode(WFMode _mode)
    {
        mode = _mode;
    }
    void setFailureWfid(unsigned _failure)
    {
        failure = _failure;
    }
    void setSuccessWfid(unsigned _success)
    {
        success = _success;
    }
    void setException(WorkflowException * e)
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "workflow item %u [%p] has its exception set", wfid, this);
#endif
        thisException.set(e);
    }
    void setContingencyWithin(unsigned n)
    {
        withinContingency = n;
    }

    void copy(IConstWorkflowItem const * other)
    {
        wfid = other->queryWfid();
        if(other->isScheduled())
            schedule.setown(new CCloneSchedule(other));
        Owned<IWorkflowDependencyIterator> iter = other->getDependencies();
        for(iter->first(); iter->isValid(); iter->next())
            dependencies.append(iter->query());
        type = other->queryType();
        mode = other->queryMode();
        success = other->querySuccess();
        failure = other->queryFailure();
        recovery = other->queryRecovery();
        retriesAllowed = other->queryRetriesAllowed();
        contingencyFor = other->queryContingencyFor();
        state = other->queryState();
        retriesRemaining = other->queryRetriesRemaining();
        if(state == WFStateFail)
        {
            failcode = other->queryFailCode();
            failmsg.set(other->queryFailMessage());
        }
        eventName.set(other->queryEventName());
        eventExtra.set(other->queryEventExtra());
        other->getPersistName(persistName);
        persistWfid = other->queryPersistWfid();
        scheduledWfid = other->queryScheduledWfid();
        persistCopies = other->queryPersistCopies();
        persistRefresh = other->queryPersistRefresh();
        other->getCriticalName(criticalName);
        other->queryCluster(clusterName);
        other->getLabel(label);
    }
    //info set at compile time
    virtual WorkflowException * queryException() const
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "workflow item %u [%p] has its exception queried", wfid, this);
#endif
        return thisException.get();
    }
    virtual unsigned     queryContingencyWithin() const { return withinContingency; }
    virtual unsigned     queryWfid() const { return wfid; }
    virtual bool         isScheduled() const { return schedule.get() != 0; }
    virtual bool         isScheduledNow() const { return schedule && schedule->isNow(); }
    virtual IWorkflowEvent * getScheduleEvent() const { if(schedule) return schedule->getEvent(); else return NULL; }
    virtual unsigned     querySchedulePriority() const { return schedule ? schedule->queryPriority() : 0; }
    virtual bool         hasScheduleCount() const { return schedule ? schedule->hasCount() : false; }
    virtual unsigned     queryScheduleCount() const { return schedule ? schedule->queryCount() : 0; }
    virtual IWorkflowDependencyIterator * getDependencies() const { return new CCloneIterator(dependencies); }
    virtual WFType       queryType() const { return type; }
    virtual WFMode       queryMode() const { return mode; }
    virtual IStringVal & getLabel(IStringVal & val) const { val.set(label.str()); return val; }
    virtual unsigned     querySuccess() const { return success; }
    virtual unsigned     queryFailure() const { return failure; }
    virtual unsigned     queryRecovery() const { return recovery; }
    virtual unsigned     queryRetriesAllowed() const { return retriesAllowed; }
    virtual unsigned     queryContingencyFor() const { return contingencyFor; }
    virtual IStringVal & getPersistName(IStringVal & val) const { val.set(persistName.str()); return val; }
    virtual unsigned     queryPersistWfid() const { return persistWfid; }
    virtual int          queryPersistCopies() const { return persistCopies; }
    virtual bool         queryPersistRefresh() const { return persistRefresh; }
    virtual IStringVal & getCriticalName(IStringVal & val) const { val.set(criticalName.str()); return val; }
    virtual IStringVal & queryCluster(IStringVal & val) const { val.set(clusterName.str()); return val; }
    //info set at run time
    virtual unsigned     queryScheduleCountRemaining() const { return schedule ? schedule->queryCountRemaining() : 0; }
    virtual WFState      queryState() const { return state; }
    virtual unsigned     queryRetriesRemaining() const { return retriesRemaining; }
    virtual int          queryFailCode() const { return failcode; }
    virtual char const * queryFailMessage() const { return failmsg.get(); }
    virtual char const * queryEventName() const { return eventName; }
    virtual char const * queryEventExtra() const { return eventExtra; }
    virtual unsigned     queryScheduledWfid() const { return scheduledWfid; }
    virtual void         setState(WFState _state) { state = _state; }
    virtual bool         testAndDecRetries()
    {
        if(retriesRemaining == 0)
            return false;
        retriesRemaining--;
        return true;
    }
    virtual bool         decAndTestScheduleCountRemaining()
    {
        if(!schedule)
            return true;
        return schedule->decAndTestCountRemaining();
    }
    virtual void incScheduleCount()
    {
        if(schedule)
            schedule->incCountRemaining();
    }
    virtual void         setFailInfo(int code, char const * message)
    {
        failcode = code;
        failmsg.set(message);
    }
    virtual void         setEvent(const char * name, const char * extra)
    {
        eventName.set(name);
        eventExtra.set(extra);
    }
    virtual void         reset()
    {
        retriesRemaining = retriesAllowed;
        if(schedule) schedule->resetCount();
        if(isScheduled())
        {
            if(isScheduledNow())
                setState(WFStateReqd);
            else if (hasScheduleCount() && (queryScheduleCountRemaining() == 0))
                setState(WFStateDone);
            else
                setState(WFStateWait);
        }
        else if(queryType() == WFTypeRecovery)
            setState(WFStateSkip);
        else
            setState(WFStateNull);
    }
};

class CWorkflowItemIterator : public CInterface, implements IWorkflowItemIterator
{
public:
    CWorkflowItemIterator(IPropertyTree * tree) { iter.setown(tree->getElements("Item")); }
    IMPLEMENT_IINTERFACE;
    bool                first() { item.clear(); return iter->first(); }
    bool                isValid() { return iter->isValid(); }
    bool                next() { item.clear(); return iter->next(); }
    IConstWorkflowItem * query() const { if(!item) item.setown(new CWorkflowItem(iter->get())); return item.get(); }
    IWorkflowItem *     get() const { if(!item) item.setown(new CWorkflowItem(iter->get())); return item.getLink(); }
private:
    Owned<IPropertyTreeIterator> iter;
    mutable Owned<CWorkflowItem> item;
};

class CCloneWorkflowItemArray : public CInterface, implements IWorkflowItemArray
{
private:
    class ListItem
    {
    public:
        ListItem(ListItem * _next, IRuntimeWorkflowItem * _item) : next(_next), item(_item) {}
        ListItem * next;
        IRuntimeWorkflowItem * item;
    };

    class ListItemPtr : public CInterface, implements IRuntimeWorkflowItemIterator
    {
    public:
        ListItemPtr(ListItem * _start) : start(_start) { ptr = NULL; }
        IMPLEMENT_IINTERFACE;
        virtual bool         first() { ptr = start; return isValid(); }
        virtual bool         isValid() { return ptr != NULL; }
        virtual bool         next() { ptr = ptr->next; return isValid(); }
        virtual IConstWorkflowItem * query() const { return ptr->item; }
        virtual IRuntimeWorkflowItem * get() const { return LINK(ptr->item); }
    private:
        ListItem * start;
        ListItem * ptr;
    };

    void insert(CCloneWorkflowItem * item)
    {
        if(!item->isScheduled())
            return;
        if(!head)
            head = tail = new ListItem(NULL, item);
        else if(item->querySchedulePriority() > head->item->querySchedulePriority())
            head = new ListItem(head, item);
        else if(item->querySchedulePriority() <= tail->item->querySchedulePriority())
        {
            tail->next = new ListItem(NULL, item);
            tail = tail->next;
        }
        else
        {
            ListItem * finger = head;
            while(item->querySchedulePriority() <= finger->next->item->querySchedulePriority())
                finger = finger->next;
            finger->next = new ListItem(finger->next, item);
        }
    }

public:
    CCloneWorkflowItemArray(unsigned _capacity) : capacity(_capacity), head(NULL), tail(NULL) 
    {
        array = _capacity ? new CCloneWorkflowItem[_capacity] : NULL;
    }
    ~CCloneWorkflowItemArray()
    {
        ListItem * finger = head;
        while(finger)
        {
            ListItem * del = finger;
            finger = finger->next;
            delete del;
        }
        if (array)
            delete [] array;
    }

    IMPLEMENT_IINTERFACE;

    virtual void addClone(IConstWorkflowItem const * other)
    {
        unsigned wfid = other->queryWfid();
        assertex((wfid > 0) && (wfid <= capacity));
        array[wfid-1].copy(other);
        insert(&array[wfid-1]);
    }

    virtual IRuntimeWorkflowItem & queryWfid(unsigned wfid)
    {
        assertex((wfid > 0) && (wfid <= capacity));
        return array[wfid-1];
    }

    virtual unsigned count() const
    {
        return capacity;
    }
    //iterator through the scheduled items (not ALL the items)
    virtual IRuntimeWorkflowItemIterator * getSequenceIterator() { return new ListItemPtr(head); }

    virtual bool hasScheduling() const
    {
        ListItem * finger = head;
        while(finger)
        {
            if(!finger->item->isScheduledNow())
                return true;
            finger = finger->next;
        }
        return false;
    }

private:
    unsigned capacity;
    CCloneWorkflowItem * array;
    ListItem * head;
    ListItem * tail;
};

//-------------------------------------------------------------------------------------------------

WorkflowMachine::WorkflowMachine()
    : ctx(NULL), process(NULL), currentWfid(0), currentScheduledWfid(0), itemsWaiting(0), itemsUnblocked(0), condition(false), logctx(queryDummyContextLogger())
{
}

WorkflowMachine::WorkflowMachine(const IContextLogger &_logctx)
    : ctx(NULL), process(NULL), currentWfid(0), currentScheduledWfid(0), itemsWaiting(0), itemsUnblocked(0), condition(false), logctx(_logctx)
{
}


void WorkflowMachine::addSuccessors()
{
    Owned<IRuntimeWorkflowItemIterator> iter = workflow->getSequenceIterator();
    if (iter->first())
    {
        while (iter->isValid())
        {
            IConstWorkflowItem * item = iter->query();
            if(item->queryState() == WFStateReqd)
            {
                //initial call
                parentWfid = item->queryWfid();
#ifdef TRACE_WORKFLOW
                LOG(MCworkflow, "Item %u has been identified as the 'parent' item, with Reqd state", parentWfid);
#endif
                CCloneWorkflowItem thisItem;
                startItem = &thisItem;
                defineLogicalRelationships(parentWfid, startItem, false);
#ifdef TRACE_WORKFLOW
                LOG(MCworkflow, "Adding initial workflow items");
#endif
                //Logical successors to the startItem are ready be executed if they have no dependencies
                processLogicalSuccessors(*startItem);
                startItem = nullptr;
                break;
            }
            if(!iter->next()) break;
        }
    }
    assertex(parentWfid != 0);
#ifdef TRACE_WORKFLOW
    //Outputting debug info about each workflow item.
    unsigned totalDependencies = 0;
    unsigned totalActiveItems = 0;
    unsigned totalInActiveItems = 0;
    unsigned totalDependentSuccessors = 0;
    unsigned totalLogicalSuccessors = 0;
    unsigned totalConditionItems = 0;
    //iterate through the workflow items
    for(int i = 1; i <= workflow->count(); i++)
    {
        CCloneWorkflowItem & cur = queryWorkflowItem(i);
        unsigned numDep = cur.queryNumDependencies();
        unsigned numDepSuc = cur.queryNumDependentSuccessors();
        unsigned numLogSuc = cur.queryNumLogicalSuccessors();
        if(cur.isActive())
            totalActiveItems++;
        else
            totalInActiveItems++;
        totalDependencies += numDep;
        totalDependentSuccessors += numDepSuc;
        totalLogicalSuccessors += numLogSuc;
        LOG(MCworkflow, "Item %u has %u dependencies, %u dependent successors and %u logical successors", cur.queryWfid(), numDep, numDepSuc, numLogSuc);

        if(cur.queryMode() == WFModeCondition)
        {
            totalConditionItems++;
        }
    }
    //iterate throught the IntermediaryWorkflow items
    for(int i = 0;  i < logicalWorkflow.size() ; i++)
    {

        IRuntimeWorkflowItem  *tmp = logicalWorkflow[i].get();
        CCloneWorkflowItem * cur = static_cast<CCloneWorkflowItem*>(tmp);
        unsigned numDep = cur->queryNumDependencies();
        unsigned numDepSuc = cur->queryNumDependentSuccessors();
        unsigned numLogSuc = cur->queryNumLogicalSuccessors();
        if(cur->isActive())
            totalActiveItems++;
        else
            totalInActiveItems++;
        totalDependencies += numDep;
        totalDependentSuccessors += numDepSuc;
        totalLogicalSuccessors += numLogSuc;
        LOG(MCworkflow, "Runtime item %u has %u dependencies, %u dependent successors and %u logical successors", cur->queryWfid(), numDep, numDepSuc, numLogSuc);
        if(cur->queryMode() == WFModeCondition)
        {
            totalConditionItems++;
        }
    }
    LOG(MCworkflow, "Total dependencies is: %u, total dependent successors is: %u, total logical successors is: %u", totalDependencies, totalDependentSuccessors, totalLogicalSuccessors);
    LOG(MCworkflow, "Total condition items is: %u, total active items is: %u, total inactive items is %u", totalConditionItems, totalActiveItems, totalInActiveItems);
    if(totalDependencies == totalDependentSuccessors)
        LOG(MCworkflow, "dependency and dependent successor count is consistent");
    else
        LOG(MCworkflow, "dependency and dependent successor count is inconsistent");
#endif
}

CCloneWorkflowItem * WorkflowMachine::insertLogicalPredecessor(unsigned successorWfid)
{
    unsigned wfid = workflow->count() + logicalWorkflow.size()+1;
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "new predecessor workflow item %u has been created", wfid);
#endif

    CCloneWorkflowItem * predecessor = new CCloneWorkflowItem(wfid); //initialise the intermediary
    Owned<IRuntimeWorkflowItem> tmp = predecessor;
    logicalWorkflow.push_back(tmp); //adding it to the workflow array

    defineLogicalRelationships(successorWfid, predecessor, false);
    return predecessor;
}

void WorkflowMachine::defineLogicalRelationships(unsigned int wfid, CCloneWorkflowItem *logicalPredecessor, bool prevOrdered)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Called mark dependents on item %u", wfid);
#endif
    //If this condition is met, then the item will be actived before the start of the execution.
    //Any new logical relationships to activate this item are irrelevant, since they cannot activate an already active item.
    if (startItem->hasLogicalSuccessor(wfid))
        return;
    //If this condition is met, then an identical call to defineLogicalRelationships has been made previously.
    //Processing it twice is redundant.
    if (logicalPredecessor->hasLogicalSuccessor(wfid))
        return;

    CCloneWorkflowItem & item = queryWorkflowItem(wfid);
    bool alreadyProcessed = (!item.isDependentSuccessorsEmpty());
    //Ordered causes the effect of logicalPredecessor to skip a generation (to this item's dependencies)
    if(!prevOrdered)
    {
        logicalPredecessor->addLogicalSuccessor(&item);
    }

    Owned<IWorkflowDependencyIterator> iter = item.getDependencies();
    //For Non-Condition items
    if(item.queryMode() != WFModeCondition)
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Item %u is a non-condition item", wfid);
#endif

        CCloneWorkflowItem * prev = nullptr;
        bool thisOrdered = false;
        bool onlyProcessFirst = false;
        for(iter->first(); iter->isValid(); iter->next())
        {
            CCloneWorkflowItem & cur = queryWorkflowItem(iter->query());
            //prev is the logical predecessor to cur.
            switch(item.queryMode())
            {
            case WFModeOrdered:
                if(prev)
                {
                    //Note: thisOrdered is false for the first ORDERED action
                    thisOrdered = true;
                    if(!alreadyProcessed)
                        cur.addLogicalSuccessor(prev);
                }
                //Note: Ordered doesn't change logicalPredecessor
                break;
            case WFModeSequential:
                onlyProcessFirst = alreadyProcessed;
                if(prev)
                    //Note: Sequential changes logicalPredecessor, so that the dependencies of the cur item also depend on prev.
                    logicalPredecessor = prev;
                break;
            }
            defineLogicalRelationships(cur.queryWfid(), logicalPredecessor, thisOrdered);
            if(onlyProcessFirst)
                return;
            if(!alreadyProcessed)
                cur.addDependentSuccessor(&item);//this means that alreadyProcessed will be true when next evaluated
            prev = &cur;
        }
    }
    else
    {
        //For Condition items
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Item %u is a condition item", wfid);
#endif
        if(!iter->first())
            throwUnexpected();
        CCloneWorkflowItem & conditionExpression = queryWorkflowItem(iter->query());
        defineLogicalRelationships(conditionExpression.queryWfid(), logicalPredecessor, false);
        if(alreadyProcessed)
            return;
        if(!iter->next())
            throwUnexpected();
        unsigned wfidTrue = iter->query();
        unsigned wfidFalse = 0;
        if(iter->next())
            wfidFalse = iter->query();

        conditionExpression.setMode(WFModeConditionExpression);
        conditionExpression.addLogicalSuccessor(insertLogicalPredecessor(wfidTrue));
        CCloneWorkflowItem & trueSuccessor = queryWorkflowItem(wfidTrue);
        trueSuccessor.addDependentSuccessor(&item);

        if(wfidFalse)
        {
            conditionExpression.addLogicalSuccessor(insertLogicalPredecessor(wfidFalse));
            CCloneWorkflowItem & falseSuccessor = queryWorkflowItem(wfidFalse);
            falseSuccessor.addDependentSuccessor(&item);
            //Decrement this.numDependencies by one, to account for one path not being completed in the future.
            item.atomicDecNumDependencies();
        }
        conditionExpression.addDependentSuccessor(&item);
    }
    //Contingency clauses (belonging to any type of item)
    //Here, an intermediary item is inserted between "item" and its contingency.
    unsigned successWfid = item.querySuccess();
    if(successWfid)
    {
        item.setSuccessWfid(insertLogicalPredecessor(successWfid)->queryWfid());
    }
    unsigned failureWfid = item.queryFailure();
    if(failureWfid)
    {
        item.setFailureWfid(insertLogicalPredecessor(failureWfid)->queryWfid());
    }
}

void WorkflowMachine::addToItemQueue(unsigned wfid)
{
    {
        CriticalBlock thisBlock(queueCritSec);
        wfItemQueue.push(wfid);
    }
    wfItemQueueSem.signal(1);
}

void WorkflowMachine::processDependentSuccessors(CCloneWorkflowItem &item)
{
    //item will never be re-executed, so this function is only called once per item
    if(item.queryWfid() == parentWfid)
    {
        //update stop conditions
        parentReached = true;
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Reached parent");
#endif
        //Evaluate stop conditions. If the workflow has failed, then it needs to check whether there are any pending contingencies
        checkIfDone();
    }
    WorkflowException * e = item.queryException();
    Owned<IWorkflowDependencyIterator> iter = item.getDependentSuccessors();
    //MORE: optionally check "alive" - could increase speed, but may introduce race condition
    for(iter->first();iter->isValid(); iter->next())
    {
        unsigned thisWfid = iter->query();
        CCloneWorkflowItem & cur = queryWorkflowItem(thisWfid);
        //this must be done even if the workflow item has an exception
        unsigned numPred = cur.atomicDecNumDependencies();
        if(e)
        {
            bool newBranch = false;
            {
                //this protects against two threads adding the same item at the same time
                CriticalBlock thisBlock(exceptionCritSec);
                if(!cur.queryException())
                {
                    cur.setException(e);
                    newBranch = true;
                }
            }
            if(newBranch)
            {
                //only process the exception if cur is active
                if(cur.isActive())
                {
                    branchCount++;
                    addToItemQueue(thisWfid);
                }
            }
        }
        else
        {
            if((numPred == 1) && cur.isActive())
            {
                addToItemQueue(thisWfid);
            }
        }
    }
    if(item.queryException())
    {
        //decrement branch count by one, since this item is already on a failed branch
        branchCount.fetch_add(-1);
        checkIfDone();
    }
}

void WorkflowMachine::processLogicalSuccessors(CCloneWorkflowItem &item)
{
    Owned<IWorkflowDependencyIterator> iter = item.getLogicalSuccessors();
    for(iter->first();iter->isValid(); iter->next())
    {
        unsigned thisWfid = iter->query();
        CCloneWorkflowItem & cur = queryWorkflowItem(thisWfid);

        bool itemIsReady = false;
        {
            //this protects against two threads activating the same item at the same time
            CriticalBlock thisBlock(activationCritSec);
            if(!cur.queryContingencyWithin())
            {
                //This may make cur alive if it was dead
                //If cur has already been deactivated by executeItemParallel(), it will soon be re-added to the item queue
                //If not, it may now never become deactivated, since it is part of a contingency (in the case that item is part of a contingency)
                //In the case that item is also not part of a contingency, then no variables have been modified
                cur.setContingencyWithin(item.queryContingencyWithin());
            }
            if(!cur.isActive())
            {
                cur.activate();
                itemIsReady = true;
            }
        }
        if(itemIsReady)
        {
            if(cur.queryNumDependencies() == 0)
            {
                addToItemQueue(thisWfid);
            }
        }
    }
}

bool WorkflowMachine::activateFailureContingency(CCloneWorkflowItem & item)
{
    unsigned failureWfid = item.queryFailure();
    if(failureWfid)
    {
        startContingency();
        CCloneWorkflowItem & failureActivator = queryWorkflowItem(failureWfid);
        failureActivator.setContingencyWithin(item.queryWfid());
        processLogicalSuccessors(failureActivator);
        return true;
    }
    return false;
}

void WorkflowMachine::checkAbort(CCloneWorkflowItem & item, bool depFailed)
{
    if(item.queryContingencyWithin())
        return;
    CriticalBlock thisBlock(exceptionCritSec);
    if(!abort)
    {
        //This stores the error that causes the workflow to abort
        runtimeError.set(item.queryException());
#ifdef TRACE_WORKFLOW
        if(!depFailed)
            LOG(MCworkflow, "Workflow item %u failed. Aborting task", item.queryWfid());
        else
            LOG(MCworkflow, "Dependency of Workflow item %u failed. Aborting task", item.queryWfid());
#endif
        abort = true;
    }
}

void WorkflowMachine::startContingency()
{
    activeContingencies++;
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Starting a new contingency");
#endif
}

void WorkflowMachine::endContingency()
{
    activeContingencies--;
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Ending a contingency");
#endif
}

void WorkflowMachine::executeItemParallel(unsigned wfid)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Beginning workflow item %u", wfid);
#endif

    CCloneWorkflowItem & item = queryWorkflowItem(wfid);
    {
        //the critical section ensures that the item is never abandoned at the same time that a different thread would have added it to the item queue.
        //this would cause a problem where the item never gets performed.
        CriticalBlock thisBlock(activationCritSec);
        bool alive = ((!abort) || item.queryContingencyWithin() || item.queryException());
        if(!alive)
        {
#ifdef TRACE_WORKFLOW
            LOG(MCworkflow, "Ignoring workflow item %u due to abort", wfid);
#endif
            //item is deactivated because it is no longer alive
            item.deactivate();
            return;
        }
    }
    switch(item.queryState())
    {
    case WFStateDone:
    case WFStateFail:
        throw new WorkflowException(WFERR_ExecutingItemMoreThanOnce, "INTERNAL ERROR: attempting to execute workflow item more than once", wfid, WorkflowException::SYSTEM, MSGAUD_user);
    case WFStateSkip:
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Nothing to be done for workflow item %u", wfid);
#endif
        return;
    case WFStateWait:
        throw new WorkflowException(WFERR_ExecutingInWaitState, "INTERNAL ERROR: attempting to execute workflow item in wait state", wfid, WorkflowException::SYSTEM, MSGAUD_user);
    case WFStateBlocked:
        throw new WorkflowException(WFERR_ExecutingInBlockedState, "INTERNAL ERROR: attempting to execute workflow item in blocked state", wfid, WorkflowException::SYSTEM, MSGAUD_user);
    }
    if(item.queryException())
    {
        checkAbort(item, true);
        item.setState(WFStateFail);
        bool hasContingency = activateFailureContingency(item);
        if(hasContingency)
            return;
        if(item.queryContingencyFor())
        {
            bool success = false;
            switch(item.queryType())
            {
            case WFTypeSuccess:
                success = true;
                //fall through
            case WFTypeFailure:
                //This item must be the last item in the contingency to execute, since a contingency cannot have its own contingency
                endContingency();
                branchCount--;
                if(checkIfDone())
                    return;
                processDependentSuccessors(queryWorkflowItem(item.queryContingencyFor()));
                if(success)
                    processLogicalSuccessors(queryWorkflowItem(item.queryContingencyFor()));
                return;
            }
        }
        processDependentSuccessors(item);
        return;
    }
    else if(!item.isActive())
    {
        //should never happen
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Ignoring workflow item %u due to inactive state", wfid);
#endif
        throwUnexpected();
    }
    else
    {
        try
        {
            switch(item.queryMode())
            {
            case WFModeNormal:
            case WFModeOnce:
                doExecuteItemParallel(item);
                break;
            case WFModeCondition:
            case WFModeSequential:
            case WFModeParallel:
                break;
            case WFModeConditionExpression:
                doExecuteConditionExpression(item);
                break;
            case WFModePersist:
                doExecutePersistItem(item);
                break;
            case WFModeCritical:
            case WFModeBeginWait:
            case WFModeWait:
                throwUnexpected();
            default:
                throwUnexpected();
            }
            item.setState(WFStateDone);
            unsigned successWfid = item.querySuccess();
            if(successWfid)
            {
                startContingency();
                CCloneWorkflowItem & successActivator = queryWorkflowItem(successWfid);
                successActivator.setContingencyWithin(item.queryWfid());
                processLogicalSuccessors(successActivator);
                return;
            }
        }
        catch(WorkflowException * e)
        {
            Owned<WorkflowException> savedException = e;
            bool hasContingency = handleFailureParallel(item, e);
            //If the contingency exists, it must be fully performed before processSuccessors is called on the current item
            //Until the clause finishes, any items dependent on the current item shouldn't execute.
            if(hasContingency)
                return;
        }
    }
    if(!done)
    {
        bool success = false;
        bool alive = false;
        switch(item.queryType())
        {
        case WFTypeNormal:
            //NOTE - doesn't need to be protected by the activationCritSec
            {
                alive = ((!abort) || item.queryContingencyWithin() || item.queryException());
            }
            if(alive)
            {
                processDependentSuccessors(item);
                processLogicalSuccessors(item);
            }
            break;
        case WFTypeSuccess:
            success = true;
            //fall through
        case WFTypeFailure:
            //This item must be the last item in the contingency to execute, since a contingency cannot have its own contingency
            endContingency();
            if(item.queryException())
                branchCount--;
            if(checkIfDone())
                return;
            processDependentSuccessors(queryWorkflowItem(item.queryContingencyFor()));
            if(success)
                processLogicalSuccessors(queryWorkflowItem(item.queryContingencyFor()));
            //An item with type Success/Failure has no successors belonging to it
            return;
        }
    }
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Done workflow item %u", wfid);
#endif
}

void WorkflowMachine::doExecuteItemParallel(IRuntimeWorkflowItem & item)
{
    try
    {
        performItemParallel(item.queryWfid());
    }
    catch(WorkflowException * ein)
    {
        if (ein->queryWfid() == 0)
        {
            StringBuffer msg;
            ein->errorMessage(msg);
            WorkflowException * newException = new WorkflowException(ein->errorCode(), msg.str(), item.queryWfid(), ein->queryType(), ein->errorAudience());
            ein->Release();
            ein = newException;
        }

        if(ein->queryType() == WorkflowException::ABORT)
            throw ein;
        //recovery will be added in a subsequent PR (Jira issue HPCC-24261)
        //if(!attemptRetry(item, 0, scheduledWfid))
        {
            throw ein;
        }
        ein->Release();
    }
    catch(IException * ein)
    {
        checkForAbort(item.queryWfid(), ein);
        //if(!attemptRetry(item, 0, scheduledWfid))
        {
            StringBuffer msg;
            ein->errorMessage(msg);
            WorkflowException::Type type = ((ein != NULL) ? WorkflowException::USER : WorkflowException::SYSTEM);
            WorkflowException * eout = new WorkflowException(ein->errorCode(), msg.str(), item.queryWfid(), type, ein->errorAudience());
            ein->Release();
            throw eout;
        }
        ein->Release();
    }
}

void WorkflowMachine::doExecuteConditionExpression(CCloneWorkflowItem & item)
{
    bool result;
    {
        //To prevent the callback that modifies "condition" from having a race condition
        CriticalBlock thisBlock(conditionCritSec);
        doExecuteItemParallel(item);
        result = condition;
    }
    //index 0 contains true successor, index 1 contains false successor
    Owned<IWorkflowDependencyIterator> iter = item.getLogicalSuccessors();
    if(!iter->first())
        throwUnexpected();
    unsigned wfidTrue = iter->query();
    unsigned wfidFalse = 0;
    if(iter->next())
        wfidFalse = iter->query();
    if(result)
    {
        CCloneWorkflowItem &trueActivator = queryWorkflowItem(wfidTrue);
        trueActivator.setContingencyWithin(item.queryContingencyWithin());
        processLogicalSuccessors(trueActivator);
    }
    else
    {
        if(wfidFalse)
        {
            CCloneWorkflowItem &falseActivator = queryWorkflowItem(wfidFalse);
            falseActivator.setContingencyWithin(item.queryContingencyWithin());
            processLogicalSuccessors(falseActivator);
        }
        else
        {
            //This function will be called again later. It is called twice, so that the parent condition item has the correct number of dependencies decremented.
            processDependentSuccessors(item);
        }
    }
    item.removeLogicalSuccessors();
}

void WorkflowMachine::performItemParallel(unsigned wfid)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Performing workflow item %u", wfid);
#endif
    timestamp_type startTime = getTimeStampNowValue();
    CCycleTimer timer;
    process->perform(ctx, wfid);
    noteTiming(wfid, startTime, timer.elapsedNs());
}

bool WorkflowMachine::handleFailureParallel(CCloneWorkflowItem & item, WorkflowException * e)
{
    item.setException(e);
    branchCount++;
    StringBuffer msg;
    e->errorMessage(msg).append(" (in item ").append(e->queryWfid()).append(")");
    logctx.logOperatorException(NULL, NULL, 0, "%d: %s", e->errorCode(), msg.str());
    item.setFailInfo(e->errorCode(), msg.str());

    item.setState(WFStateFail);
    if(!item.queryContingencyWithin())
    {
        checkAbort(item, false);
    }
    else
    {
        WFState contingencyState = queryWorkflowItem(item.queryContingencyWithin()).queryState();
        if(contingencyState == WFStateDone)
            reportContingencyFailure("SUCCESS", e);
        else if(contingencyState == WFStateFail)
            reportContingencyFailure("FAILURE", e);
        else
            reportContingencyFailure("Unknown", e);
    }
    return activateFailureContingency(item);
}

CCloneWorkflowItem &WorkflowMachine::queryWorkflowItem(unsigned wfid)
{
    if(wfid <= workflow->count())
    {
        return static_cast<CCloneWorkflowItem&>(workflow->queryWfid(wfid));
    }
    else
    {
        unsigned index = wfid - workflow->count() - 1;
        if(index >= logicalWorkflow.size())
            throwUnexpected();
        return static_cast<CCloneWorkflowItem&>(*logicalWorkflow[index].get());
    }
}

bool WorkflowMachine::checkIfDone()
{
    if((activeContingencies == 0) && (parentReached))
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "WorkflowMachine::checkifDone. Final check. Branch count: %u", branchCount.load());
#endif
        if((branchCount == 0))
        {
#ifdef TRACE_WORKFLOW
            LOG(MCworkflow, "workflow done");
#endif
            done = true;
            wfItemQueueSem.signal(numThreads);
            return true;
        }
    }
    return false;
}

void WorkflowMachine::processWfItems()
{
    while(!done)
    {
        wfItemQueueSem.wait();
        if(!done)
        {
            unsigned currentWfid = 0;
            {
                CriticalBlock thisBlock(queueCritSec);
                currentWfid = wfItemQueue.front();
                wfItemQueue.pop();
            }
            try
            {
                executeItemParallel(currentWfid);
            }
            //terminate threads on fatal exception and save error
            catch(WorkflowException * e)
            {
                runtimeError.setown(e);
                done = true;
                wfItemQueueSem.signal(numThreads); //MORE: think about interrupting other threads
                break;
            }
        }
    }
}

void WorkflowMachine::performParallel(IGlobalCodeContext *_ctx, IEclProcess *_process)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "starting perform parallel");
#endif
    ctx = _ctx;
    process = _process;

    //relink workflow
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Starting to mark Items with their successors");
#endif
    addSuccessors();
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Finished marking Items with their successors");
#endif

#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Initialising threads");
#endif

    //initialise thread count
    numThreads = getThreadNumFlag();
    if(numThreads < 1)
        numThreads = 4;
    unsigned maxThreads = getAffinityCpus();
    if(numThreads > maxThreads)
        numThreads = maxThreads;
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "num threads = %u", numThreads);
#endif

    std::vector<std::thread *> threads(numThreads);
    //NOTE: Initial work items have already been added to the queue by addSuccessors (above)
    //Start threads
    for(int i=0; i < numThreads; i++)
        threads[i] = new std::thread([this]() {  this->processWfItems(); });

#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Calling join threads");
#endif
    //wait for threads to process the workflow items, and then exit when all the work is done
    for(int i=0; i < numThreads; i++)
        threads[i]->join();

#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Destroying threads");
#endif
    for(int i=0; i < numThreads; i++)
        delete threads[i];

    if(runtimeError)
        throw runtimeError.getClear();
}

bool WorkflowMachine::isParallelViable()
{
    //initialise parallel flag from workunit
    parallel = getParallelFlag();
    if(!parallel)
    {
        return false;
    }
    for(int i = 1; i <= workflow->count(); i++)
    {
        CCloneWorkflowItem & cur = queryWorkflowItem(i);
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Checking Item %u to decide if parallel is viable", i);
#endif
        //list of exceptions for currently unsupported modes/types
        switch(cur.queryMode())
        {
        case WFModeWait:
        case WFModeBeginWait:
        case WFModeCritical:
        case WFModePersist:
        case WFModeOnce:
            return false;
        }
        switch(cur.queryType())
        {
        case WFTypeRecovery:
            return false;
        }
        //switch(cur.queryState())
        if(cur.isScheduled() && (!cur.isScheduledNow()))
            return false;
    }
    return true;
}

//The process parameter defines the c++ task associated with each workflowItem
//These are executed in the context/scope of the 'agent' which calls perform()
void WorkflowMachine::perform(IGlobalCodeContext *_ctx, IEclProcess *_process)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "starting perform");
#endif
    ctx = _ctx;
    process = _process;

    //This is where the 'agent' initialises the workflow engine with an array of workflowItems, with their dependencies
    begin();

    if(isParallelViable())
    {
        performParallel(_ctx, _process);
        return;
    }
    Owned<WorkflowException> error;
    bool scheduling = workflow->hasScheduling();
    if(scheduling)
        schedulingStart();
    bool more = false;
    do
    {
        Owned<IRuntimeWorkflowItem> item;
        Owned<IRuntimeWorkflowItemIterator> iter = workflow->getSequenceIterator();
        itemsWaiting = 0;
        itemsUnblocked = 0;
        if (iter->first())
        {
            while (iter->isValid())
            {
                try
                {
                    item.setown(iter->get());
                    switch(item->queryState())
                    {
                    case WFStateReqd:
                    case WFStateFail:
                        if(!error)
                        {
                            unsigned wfid = item->queryWfid();
                            executeItem(wfid, wfid);
                        }
                        break;
                    }
                }
                catch(WorkflowException * e)
                {
                    error.setown(e);
                }
                if(item->queryState() == WFStateWait) itemsWaiting++;
                if(error) break; //MORE: will not want to break in situations where there might be pending contingency clauses
                if(scheduling && schedulingPull())
                {
                    itemsWaiting = 0;
                    iter.setown(workflow->getSequenceIterator());
                    if(!iter->first()) break;
                }
                else
                    if(!iter->next()) break;
            }
        }
        if(error) break; //MORE: will not want to break in situations where there might be pending contingency clauses
        if(scheduling)
            more = schedulingPullStop();
    } while(more || itemsUnblocked);
    end();
    if(error)
        throw error.getLink();
}

bool WorkflowMachine::executeItem(unsigned wfid, unsigned scheduledWfid)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Beginning workflow item %u", wfid);
#endif
    IRuntimeWorkflowItem & item = workflow->queryWfid(wfid);
    switch(item.queryState())
    {
    case WFStateDone:
        if (item.queryMode() == WFModePersist)
        {
#ifdef TRACE_WORKFLOW
            LOG(MCworkflow, "Recheck persist %u", wfid);
#endif
            break;
        }
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Nothing to be done for workflow item %u", wfid);
#endif
        return true;
    case WFStateSkip:
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Nothing to be done for workflow item %u", wfid);
#endif
        return true;
    case WFStateWait:
        throw new WorkflowException(WFERR_ExecutingInWaitState, "INTERNAL ERROR: attempting to execute workflow item in wait state", wfid, WorkflowException::SYSTEM, MSGAUD_user);
    case WFStateBlocked:
        throw new WorkflowException(WFERR_ExecutingInBlockedState, "INTERNAL ERROR: attempting to execute workflow item in blocked state", wfid, WorkflowException::SYSTEM, MSGAUD_user);
    case WFStateFail:
        item.reset();
        break;
    }

    switch(item.queryMode())
    {
    case WFModeNormal:
    case WFModeOnce:
        if (!doExecuteItemDependencies(item, wfid))
            return false;
        doExecuteItem(item, scheduledWfid);
        break;
    case WFModeCondition:
        if (!doExecuteConditionItem(item, scheduledWfid))
            return false;
        break;
    case WFModeSequential:
    case WFModeParallel:
        if (!doExecuteItemDependencies(item, scheduledWfid))
            return false;
        break;
    case WFModePersist:
        doExecutePersistItem(item);
        break;
    case WFModeCritical:
        doExecuteCriticalItem(item);
        break;
    case WFModeBeginWait:
        doExecuteBeginWaitItem(item, scheduledWfid);
        item.setState(WFStateDone);
        return false;
    case WFModeWait:
        doExecuteEndWaitItem(item);
        break;
    default:
        throwUnexpected();
    }

    switch(item.queryType())
    {
    case WFTypeNormal:
        if(item.isScheduled() && !item.isScheduledNow() && item.decAndTestScheduleCountRemaining())
            item.setState(WFStateWait);
        else
            item.setState(WFStateDone);
        break;
    case WFTypeSuccess:
    case WFTypeFailure:
        item.setState(WFStateNull);
        break;
    case WFTypeRecovery:
        item.setState(WFStateSkip);
        break;
    }
    if(item.querySuccess())
    {
        try
        {
            executeItem(item.querySuccess(), scheduledWfid);
        }
        catch(WorkflowException * ce)
        {
            if(ce->queryType() == WorkflowException::ABORT)
                throw;
            reportContingencyFailure("SUCCESS", ce);
            ce->Release();
        }
    }
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Done workflow item %u", wfid);
#endif
    return true;
}

bool WorkflowMachine::doExecuteItemDependencies(IRuntimeWorkflowItem & item, unsigned scheduledWfid)
{
    Owned<IWorkflowDependencyIterator> iter = item.getDependencies();
    for(iter->first(); iter->isValid(); iter->next())
    {
        if (!doExecuteItemDependency(item, iter->query(), scheduledWfid, false))
            return false;
    }
    return true;
}

bool WorkflowMachine::doExecuteItemDependency(IRuntimeWorkflowItem & item, unsigned wfid, unsigned scheduledWfid, bool alwaysEvaluate)
{
    try
    {
        if (alwaysEvaluate)
            workflow->queryWfid(wfid).setState(WFStateNull);

        return executeItem(wfid, scheduledWfid);
    }
    catch(WorkflowException * e)
    {
        if(e->queryType() == WorkflowException::ABORT)
            throw;
        if(!attemptRetry(item, wfid, scheduledWfid))
        {
            handleFailure(item, e, true);
            throw;
        }
        e->Release();
    }
    return true;//more!
}

void WorkflowMachine::doExecuteItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid)
{
    try
    {
        performItem(item.queryWfid(), scheduledWfid);
    }
    catch(WorkflowException * ein)
    {
        if (ein->queryWfid() == 0)
        {
            StringBuffer msg;
            ein->errorMessage(msg);
            WorkflowException * newException = new WorkflowException(ein->errorCode(), msg.str(), item.queryWfid(), ein->queryType(), ein->errorAudience());
            ein->Release();
            ein = newException;
        }

        if(ein->queryType() == WorkflowException::ABORT)
            throw ein;

        if(!attemptRetry(item, 0, scheduledWfid))
        {
            handleFailure(item, ein, true);
            throw ein;
        }
        ein->Release();
    }
    catch(IException * ein)
    {
        checkForAbort(item.queryWfid(), ein);
        if(!attemptRetry(item, 0, scheduledWfid))
        {
            StringBuffer msg;
            ein->errorMessage(msg);
            WorkflowException::Type type = ((dynamic_cast<IUserException *>(ein) != NULL) ? WorkflowException::USER : WorkflowException::SYSTEM);
            WorkflowException * eout = new WorkflowException(ein->errorCode(), msg.str(), item.queryWfid(), type, ein->errorAudience());
            ein->Release();
            handleFailure(item, eout, false);
            throw eout;
        }
        ein->Release();
    }
}

bool WorkflowMachine::doExecuteConditionItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid)
{
    Owned<IWorkflowDependencyIterator> iter = item.getDependencies();
    if(!iter->first()) throwUnexpected();
    unsigned wfidCondition = iter->query();
    if(!iter->next()) throwUnexpected();
    unsigned wfidTrue = iter->query();
    unsigned wfidFalse = 0;
    if(iter->next()) wfidFalse = iter->query();
    if(iter->next()) throwUnexpected();

    if (!doExecuteItemDependency(item, wfidCondition, scheduledWfid, true))
        return false;
    if(condition)
        return doExecuteItemDependency(item, wfidTrue, scheduledWfid, false);
    else if (wfidFalse)
        return doExecuteItemDependency(item, wfidFalse, scheduledWfid, false);
    return true;
}

void WorkflowMachine::doExecuteBeginWaitItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid)
{
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Begin wait for workflow item %u sched %u", item.queryWfid(), scheduledWfid);
#endif
    //Block execution of the currently executing scheduled item
    IRuntimeWorkflowItem & scheduledItem = workflow->queryWfid(scheduledWfid);
    assertex(scheduledItem.queryState() == WFStateReqd);
    scheduledItem.setState(WFStateBlocked);

    //And increment the count on the wait wf item so it becomes active
    Owned<IWorkflowDependencyIterator> iter = item.getDependencies();
    if(!iter->first()) throwUnexpected();
    unsigned waitWfid = iter->query();
    if(iter->next()) throwUnexpected();

    IRuntimeWorkflowItem & waitItem = workflow->queryWfid(waitWfid);
    assertex(waitItem.queryState() == WFStateDone);
    waitItem.incScheduleCount();
    waitItem.setState(WFStateWait);
    itemsWaiting++;
}

void WorkflowMachine::doExecuteEndWaitItem(IRuntimeWorkflowItem & item)
{
    //Unblock the scheduled workflow item, which should mean execution continues.
    unsigned scheduledWfid = item.queryScheduledWfid();
#ifdef TRACE_WORKFLOW
    LOG(MCworkflow, "Finished wait for workflow sched %u", scheduledWfid);
#endif
    IRuntimeWorkflowItem & scheduledItem = workflow->queryWfid(scheduledWfid);
    assertex(scheduledItem.queryState() == WFStateBlocked);
    scheduledItem.setState(WFStateReqd);
    itemsUnblocked++;

    //Note this would be more efficient implemented more like a state machine 
    //(with next processing rather than walking from the top down), 
    //but that will require some more work.
}


bool WorkflowMachine::isOlderThanPersist(time_t when, IRuntimeWorkflowItem & item)
{
    time_t thisTime;
    if (!getPersistTime(thisTime, item))
        return false;  // if no time must be older than the persist
    return when < thisTime;
}

bool WorkflowMachine::isOlderThanInputPersists(time_t when, IRuntimeWorkflowItem & item)
{
    Owned<IWorkflowDependencyIterator> iter = item.getDependencies();
    ForEach(*iter)
    {
        unsigned cur = iter->query();

        IRuntimeWorkflowItem & other = workflow->queryWfid(cur);
        if (isPersist(other))
        {
            if (isOlderThanPersist(when, other))
                return true;
        }
        else
        {
            if (isOlderThanInputPersists(when, other))
                return true;
        }
    }
    return false;
}

bool WorkflowMachine::isItemOlderThanInputPersists(IRuntimeWorkflowItem & item)
{
    time_t curWhen;
    if (!getPersistTime(curWhen, item))
        return false; // if no time then old and can't tell

    return isOlderThanInputPersists(curWhen, item);
}

void WorkflowMachine::performItem(unsigned wfid, unsigned scheduledWfid)
{
#ifdef TRACE_WORKFLOW
    if(currentWfid)
        LOG(MCworkflow, "Branching from workflow item %u", currentWfid);
    LOG(MCworkflow, "Performing workflow item %u", wfid);
#endif
    wfidStack.append(currentWfid);
    wfidStack.append(scheduledWfid);
    currentWfid = wfid;
    currentScheduledWfid = scheduledWfid;
    timestamp_type startTime = getTimeStampNowValue();
    CCycleTimer timer;
    process->perform(ctx, wfid);
    noteTiming(wfid, startTime, timer.elapsedNs());
    scheduledWfid = wfidStack.popGet();
    currentWfid = wfidStack.popGet();
    if(currentWfid)
    {
#ifdef TRACE_WORKFLOW
        LOG(MCworkflow, "Returning to workflow item %u", currentWfid);
#endif
    }
}

bool WorkflowMachine::attemptRetry(IRuntimeWorkflowItem & item, unsigned dep, unsigned scheduledWfid)
{
    unsigned wfid = item.queryWfid();
    unsigned recovery = item.queryRecovery();
    if(!recovery)
        return false;
    while(item.testAndDecRetries())
    {
        bool okay = true;
        try
        {
            workflow->queryWfid(recovery).setState(WFStateNull);
            executeItem(recovery, recovery);
            if(dep)
                executeItem(dep, scheduledWfid);
            else
                performItem(wfid, scheduledWfid);
        }
        catch(WorkflowException * ce)
        {
            okay = false;
            if(ce->queryType() == WorkflowException::ABORT)
                throw;
            reportContingencyFailure("RECOVERY", ce);
            ce->Release();
        }
        catch(IException * ce)
        {
            okay = false;
            checkForAbort(wfid, ce);
            reportContingencyFailure("RECOVERY", ce);
            ce->Release();
        }
        if(okay)
            return true;
    }
    return false;
}

void WorkflowMachine::handleFailure(IRuntimeWorkflowItem & item, WorkflowException const * e, bool isDep)
{
    StringBuffer msg;
    e->errorMessage(msg).append(" (in item ").append(e->queryWfid()).append(")");
    if(isDep)
        logctx.logOperatorException(NULL, NULL, 0, "Dependency failure for workflow item %u: %d: %s", item.queryWfid(), e->errorCode(), msg.str());
    else
        logctx.logOperatorException(NULL, NULL, 0, "%d: %s", e->errorCode(), msg.str());
    item.setFailInfo(e->errorCode(), msg.str());
    switch(item.queryType())
    {
    case WFTypeNormal:
        item.setState(WFStateFail);
        break;
    case WFTypeSuccess:
    case WFTypeFailure:
        item.setState(WFStateNull);
        break;
    case WFTypeRecovery:
        item.setState(WFStateSkip);
        break;
    }
    unsigned failureWfid = item.queryFailure();
    if(failureWfid)
    {
        try
        {
            executeItem(failureWfid, failureWfid);
        }
        catch(WorkflowException * ce)
        {
            if(ce->queryType() == WorkflowException::ABORT)
                throw;
            reportContingencyFailure("FAILURE", ce);
            ce->Release();
        }
    }
}

int WorkflowMachine::queryLastFailCode() const
{
    unsigned wfidFor = workflow->queryWfid(currentWfid).queryContingencyFor();
    if(!wfidFor)
        return 0;
    return workflow->queryWfid(wfidFor).queryFailCode();
}

char const * WorkflowMachine::queryLastFailMessage() const
{
    unsigned wfidFor = workflow->queryWfid(currentWfid).queryContingencyFor();
    if(!wfidFor)
        return "";
    char const * ret = workflow->queryWfid(wfidFor).queryFailMessage();
    return ret ? ret : "";
}

const char * WorkflowMachine::queryEventName() const
{
    //MORE: This doesn't work so well once we've done SEQUENTIAL transforms if they split a wf item into 2
    return workflow->queryWfid(currentWfid).queryEventName();
}

const char * WorkflowMachine::queryEventExtra() const
{
    //MORE: This doesn't work so well once we've done SEQUENTIAL transforms if they split a wf item into 2
    return workflow->queryWfid(currentWfid).queryEventExtra();
}


IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree *p)
{
    return new CWorkflowItemIterator(p);
}

IWorkflowItemArray *createWorkflowItemArray(unsigned size)
{
    return new CCloneWorkflowItemArray(size);
}

IWorkflowItem *createWorkflowItem(IPropertyTree * ptree, unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor)
{
    return new CWorkflowItem(ptree, wfid, type, mode, success, failure, recovery, retriesAllowed, contingencyFor);
}
