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
// Workflow

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
    Owned<CCloneSchedule> schedule;
    IntArray dependencies;
    WFType type;
    WFMode mode;
    unsigned success;
    unsigned failure;
    unsigned recovery;
    unsigned retriesAllowed;
    unsigned contingencyFor;
    unsigned scheduledWfid;
    WFState state;
    unsigned retriesRemaining;
    int failcode;
    StringAttr failmsg;
    SCMStringBuffer persistName;
    SCMStringBuffer clusterName;
    SCMStringBuffer label;
    unsigned persistWfid;
    int persistCopies;
    bool persistRefresh;
    SCMStringBuffer criticalName;
    StringAttr eventName;
    StringAttr eventExtra;

public:
    CCloneWorkflowItem() : persistRefresh(true) {}
    IMPLEMENT_IINTERFACE;
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

#ifdef TRACE_WORKFLOW
const LogMsgCategory MCworkflow = MCuserProgress(50);       // Category used to inform enqueue/start/finish of workflow item
#endif

WorkflowMachine::WorkflowMachine()
    : ctx(NULL), process(NULL), currentWfid(0), currentScheduledWfid(0), itemsWaiting(0), itemsUnblocked(0), condition(false), logctx(queryDummyContextLogger())
{
}

WorkflowMachine::WorkflowMachine(const IContextLogger &_logctx)
    : ctx(NULL), process(NULL), currentWfid(0), currentScheduledWfid(0), itemsWaiting(0), itemsUnblocked(0), condition(false), logctx(_logctx)
{
}

void WorkflowMachine::perform(IGlobalCodeContext *_ctx, IEclProcess *_process)
{
    ctx = _ctx;
    process = _process;
    Owned<WorkflowException> error;
    begin();
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
        throw new WorkflowException(0, "INTERNAL ERROR: attempting to execute workflow item in wait state", wfid, WorkflowException::SYSTEM, MSGAUD_user);
    case WFStateBlocked:
        throw new WorkflowException(0, "INTERNAL ERROR: attempting to execute workflow item in blocked state", wfid, WorkflowException::SYSTEM, MSGAUD_user);
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
        if(ein->queryType() == WorkflowException::ABORT)
            throw;
        if(!attemptRetry(item, 0, scheduledWfid))
        {
            handleFailure(item, ein, true);
            throw;
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
