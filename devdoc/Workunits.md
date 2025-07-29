# Understanding Workunits

## What is a Workunit?

A workunit contains all the information that the HPCC Systems® platform requires about a query, including the parameters it takes, how to execute it, and how to format the results. Understanding workunit contents is essential for understanding how the HPCC Systems platform components work together.

## Who Should Read This Document

This document is intended for:

- Platform developers who need to understand internal workunit structure
- Advanced users debugging query execution
- Contributors working on ECL compiler or engine components

## Overview

This document provides an overview of workunit elements, followed by a walkthrough of executing a simple query with detailed descriptions of workunit components.

## Understanding HPCC Systems® Platform Architecture

The HPCC Systems platform logically separates query execution into two parts:

1. **Algorithms** - The "how to do it" operations like sorting and deduplicating. These algorithms are implemented in the engines (hThor, ROXIE, and Thor) and are reused across all queries.

2. **Metadata** - The "what to do" information specific to each query, including:
   - Record format descriptions
   - Sort column specifications
   - Operation sequence requirements
   - Generated compare functions
   - Serialized data and graphs

Workunits contain only the metadata. When you examine a workunit for an ECL query that sorts a dataset, you won't find sorting code or even calls to sort library functions. That logic resides in the engine that executes the query.

This separation means execution alternates between generated code (from the workunit) and engine algorithms. This document explains how this generated code is structured and its role in query execution.

## Terminology

**Query** - A generic term covering both read-only queries (typically run in ROXIE) and ETL (Extract, Transform, and Load) queries that create persistent datafiles (typically run in Thor).

**Workunit** - Used in two contexts:

- The DLL created from a query (static)  
- A single execution instance of a query, including parameters and results (dynamic)

The context makes clear which meaning applies.

**DLL** - A generic term for dynamically loaded libraries, including shared objects (.so) in Linux, dynamic libraries (.dylib) in macOS, and dynamic link libraries (.dll) in Windows.

### Contents of a Workunit

The ECL compiler generates a workunit as a single DLL containing several elements:

- Various C++ helper classes and exported factory methods that create instances of those classes
- An XML resource with information about the workunit, including workflow and graphs
- Other user-defined resources included in the manifest

The workunit DLL contains everything the engines need to execute the query. When you execute a workunit, the system clones key XML elements from the workunit DLL and copies them into a database. The system then augments this information as the query executes with input parameters, results, statistics, and other data. Access the workunit contents through the `IWorkUnit` interface (defined in `common/workunit/workunit.hpp`), which hides implementation details.

The system currently stores workunit information in the Dali database (one of the HPCC Systems platform components). Work is in progress to allow storing most workunit data in Cassandra or another third-party database instead.

### How the System Uses Workunits

Most components in the system use workunit information:

- **eclcc** - Creates a workunit DLL from an ECL query

- **eclccserver** - Executes eclcc to create a workunit DLL, then clones information into Dali to create an active instance ready for execution

- **ESP** - Uses workunit DLL information to publish workunits, including parameter details, formatting requirements, and result specifications

- **eclscheduler** - Monitors workunits waiting for events and updates them when those events occur

- **eclagent/ROXIE** - Process different workflow actions and workflow code

- **hThor/ROXIE/Thor** - Execute graphs within workflow items

- **Dali** - Stores workunit state information

### Example

The following ECL will be used as an example for the rest of the
discussion. It is a very simple search that takes a string parameter
'searchName', which is the name of the person to search for, and
returns the matching records from an index. It also outputs the word
'Done!' as a separate result.

```ecl
STRING searchName := 'Smith' : STORED('searchName');
nameIndex := INDEX({ STRING40 name, STRING80 address }, 'names');
results := nameIndex(KEYED(name = searchName));
OUTPUT(results);
OUTPUT('Done!');
```

Extracts from the XML and C++ that are generated from this example will
be included in the following discussion.

## Workunit Main Elements

This section outlines the different sections in a workunit. This is
followed by a walk-through of the stages that take place when a workunit
is executed, together with a more detailed explanation of the workunit
contents.

### Workflow

Workflow provides the highest level of control within a workunit. Use workflow for two related purposes:

- **Scheduling** - The HPCC Systems platform allows ECL code to execute when certain events occur (for example, every hour or when files are uploaded to a landing zone). Each piece of ECL code triggered by an external event creates a separate workflow action, allowing independent processing of each event.

- **Splitting queries** - Break parts of an ECL query into independent sections when useful. The simplest example is the PERSIST workflow operation, which allows results to be shared between different workunits. Each workflow operation creates one (or sometimes more) independent workflow items, which are then connected together.

Each piece of independent ECL is given a unique workflow id (wfid).
Often workflow items need to be executed in a particular order, e.g., ensuring a persist exists before using it, which is managed with dependencies between different workflow items.

Our example above generates the following XML entry in the workunit:

```xml
<Workflow>
    <Item .... wfid="1"/>
    <Item .... wfid="2">
    <Dependency wfid="1"/>
    <Schedule/>
    </Item>
</Workflow>
```

This contains two workflow items. The first workflow item (wfid=1)
ensures that the stored value has a default value if it has not been
supplied. The second item (with wfid=2) is the main code for the query.
This has a dependency on the first workflow item because the stored
variable needs to be initialized before it is executed.

### MyProcess

The generated code contains a class instance that is used for executing
the code associated with the workflow items. It is generated at the end
of the main C++ module. For example:

```cpp
struct MyEclProcess : public EclProcess {
    virtual int perform(IGlobalCodeContext * gctx, unsigned wfid) {
        ....
        switch (wfid) {
            case 1U:
                ... code for workflow item 1 ...
            case 2U:
                ... code for workflow item 2 ...
            break;
        }
        return 2U;
    }
};
```

The main element is a switch statement inside the perform() function
that allows the workflow engines to execute the code associated with a
particular workflow item.

There is also an associated factory function that is exported from the
dll, and is used by the engines to create instances of the class:

```cpp
extern "C" ECL_API IEclProcess* createProcess()
{
    return new MyEclProcess;
}
```

### Graph

Most of the work executing a query involves processing dataset
operations, which are implemented as a graph of activities. Each graph
is represented in the workunit as an xml graph structure (currently it
uses the xgmml format). The graph xml includes details of which types of
activities are required to be executed, how they are linked together,
and any other dependencies.

The graph in our example is particularly simple:

```xml
<Graph name="graph1" type="activities">
    <xgmml>
    <graph wfid="2">
    <node id="1">
    <att>
        <graph>
        <att name="rootGraph" value="1"/>
        <edge id="2_0" source="2" target="3"/>
        <node id="2" label="Index Read&#10;&apos;names&apos;">
        ... attributes for activity 2 ...
        </node>
        <node id="3" label="Output&#10;Result #1">
        ... attributes for activity 3 ...
        </node>
        </graph>
    </att>
    </node>
    </graph>
    </xgmml>
</Graph>
```

This graph contains a single subgraph (node id=2) that contains two
activities - an index read activity and an output result activity. These
activities are linked by a single edge (id "2\_0"). The details of the
contents are covered in the section on executing graphs below.

### Generated Activity Helpers

Each activity has a corresponding class instance in the generated code,
and a factory function for creating instances of that class:

```cpp
struct cAc2 : public CThorIndexReadArg {
    ... Implementation of the helper for activity #2 ...
};
extern "C" ECL_API IHThorArg * fAc2() { return new cAc2; }

struct cAc3 : public CThorWorkUnitWriteArg {
    ... Implementation of the helper for activity #3 ...
};
extern "C" ECL_API IHThorArg * fAc3() { return new cAc3; }
```

The helper class for an activity implements the interface that is
required for that particular kind. (The interfaces are defined in
rtl/include/eclhelper.hpp - further details below.)

### Other

The are several other items, detailed below, that are logically
associated with a workunit. The information may be stored in the
workunit dll or in various other location e.g., Dali, Sasha or Cassandra.
It is all accessed through the IWorkUnit interface in
common/workunit/workunit.hpp that hides the implementation details. For
instance information generated at runtime cannot by definition be
included in the workunit dll.

### Options

Options that are supplied to eclcc via the -f command line option, or
the \#option statement are included in the \<Debug\> section of the
workunit xml:

```xml
<Debug>
    <addtimingtoworkunit>0</addtimingtoworkunit>
    <noterecordsizeingraph>1</noterecordsizeingraph>
    <showmetaingraph>1</showmetaingraph>
    <showrecordcountingraph>1</showrecordcountingraph>
    <spanmultiplecpp>0</spanmultiplecpp>
    <targetclustertype>hthor</targetclustertype>
</Debug>
```

**Note**: The names of workunit options are case insensitive, and converted
to lower case.

### Input Parameters

Many queries contain input parameters that modify their behavior. These correspond to STORED definitions in the ECL. Our example contains a single string "searchName", so the workunit contains a single input parameter:

```xml
<Variables>
    <Variable name="searchname">
    <SchemaRaw xsi:type="SOAP-ENC:base64">
        searchname&#xe000;&#xe004;&#241;&#255;&#255;&#255;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;&#xe000;&#xe018;&#xe000;&#xe000;&#xe000;&#xe000;   
    </SchemaRaw>
    </Variable>
</Variables>
```

The implementation details of the schema information is encapsulated by
the IConstWUResult interface in workunit.hpp.

### Results

The workunit XML also contains details of each result that the query generates, including a serialized description of the output record format:

```xml
<Results>
    <Result isScalar="0"
            name="Result 1"
            recordSizeEntry="mf1"
            rowLimit="-1"
            sequence="0">
    <SchemaRaw xsi:type="SOAP-ENC:base64">
    name&#xe000;&#xe004;(&#xe000;&#xe000;&#xe000;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;address&#xe000;&#xe004;P&#xe000;&#xe000;&#xe000;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;&#xe000;&#xe018;%&#xe000;&#xe000;&#xe000;{ string40 name, string80 address };&#10;   </SchemaRaw>
    </Result>
    <Result name="Result 2" sequence="1">
    <SchemaRaw xsi:type="SOAP-ENC:base64">
    Result_2&#xe000;&#xe004;&#241;&#255;&#255;&#255;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;&#xe000;&#xe018;&#xe000;&#xe000;&#xe000;&#xe000;   </SchemaRaw>
    </Result>
</Results>
```

in our example there are two - the dataset of results and the text
string "Done!". The values of the results for a query are associated
with the workunit. (They are currently saved in dali, but this may
change in version 6.0.)

### Timings and Statistics

Any timings generated when compiling the query are included in the
workunit dll:

```xml
<Statistics>
    <Statistic c="eclcc"
            count="1"
            creator="eclcc"
            kind="SizePeakMemory"
            s="compile"
            scope=">compile"
            ts="1428933081084000"
            unit="sz"
            value="27885568"/>
</Statistics>
```

Other statistics and timings created when running the query are stored in the runtime copy of the workunit. Statistics for graph elements are stored in a different format from global statistics, but the IWorkUnit interface ensures the implementation details are hidden.

### Manifests

You can include other user-defined resources in the workunit DLL, such as web pages or dashboard layouts.

## Stages of Execution

Once you compile a workunit to a DLL, it is ready for execution. You can trigger execution in different ways:

- **Submit ECL query to ESP:**
  - Create a workunit entry containing the ECL in Dali and add it to an eclccserver queue
  - An eclccserver instance removes the workunit from the queue and compiles the ECL to a workunit DLL
  - Update the Dali workunit entry with information from the workunit DLL
  - Add the Dali workunit to the agent execution queue associated with the target cluster
  - The associated engine (agentexec for hThor and Thor) pulls a query from the queue and executes it

- **Submit and publish a query with a name, then execute the named query:**
  - Create a workunit entry containing the ECL in Dali and add it to an eclccserver queue
  - An eclccserver instance removes the workunit from the queue and compiles the ECL to a workunit DLL
  - Add the new workunit DLL to the appropriate query set for the query name and target cluster combination, marking it as the current active implementation
  - Later, submit a query that references the named query to ESP
  - Map the name and target cluster via the query set to the active implementation, and create a workunit instance from the active workunit DLL
  - Add the workunit to a ROXIE or eclagentexec queue ready for execution
  - The associated engine pulls a query from the queue and executes it

- **Compile a query as a standalone executable and run it:**
  - Execute eclcc on the command line without the -shared command line option
  - Run the resulting executable. The engine used to execute the query depends on the -platform parameter supplied to eclcc

Most queries create persistent workunits in Dali and update those workunits with results as they are calculated. However, for some ROXIE queries (for example, in a production system), the execution workunits are transient.

### Queues

The system uses several inter-process queues to communicate between different components. Dali implements these queues. Components can subscribe to one or more queues and receive notifications when entries are available.

Example queues include:

- `<cluster>.eclserver` - workunits to be compiled
- `<cluster>.roxie` - workunits to execute on ROXIE
- `<cluster>.thor` - graphs to execute on Thor
- `<cluster>.eclscheduler` - workunits that need to wait for events
- `<cluster>.agent` - workunits to be executed on hThor or Thor
- `dfuserver_queue` - DFU workunits for sprays/file copies etc.

### Workflow Execution

When a workunit is ready to run, the workflow controls execution flow. The workflow engine (defined in `common/workunit/workflow.cpp`) determines which workflow item should execute next.

ECL Agent coordinates workflow for Thor and hThor jobs, while ROXIE includes the workflow engine in its process. ECL Scheduler also uses the workflow engine to process events and mark workflow items ready for execution.

ECL Agent or ROXIE calls the `createProcess()` function from the workunit DLL to create an instance of the generated workflow helper class, then passes it to the workflow engine. The workflow engine walks the workflow items to find any items ready for execution (having the state "reqd" - required). If a required workflow item has dependencies on other child workflow items, those children execute first. Once all dependencies execute successfully, the parent workflow item executes. The example has the following workflow entries:

```xml
<Workflow>
    <Item mode="normal"
        state="null"
        type="normal"
        wfid="1"/>
    <Item mode="normal"
        state="reqd"
        type="normal"
        wfid="2">
        <Dependency wfid="1"/>
        <Schedule/>
    </Item>
</Workflow>
```

Item 2 has a state of "reqd", so it should be evaluated now. Item 2
has a dependency on item 1, so that must be evaluated first. This is
achieved by calling MyEclProcess::perform() on the object that was
previously created from the workunit dll, passing in wfid = 1. That will
execute the following code:

```cpp
switch (wfid) {
    case 1U:
        if (!gctx->isResult("searchname",4294967295U)) {
            ctx->setResultString("searchname",4294967295U,5U,"Smith");
        }
        break;
    break;
}
```

This checks if a value has been provided for the input parameter, and if not assigns a default value of "Smith". The function returns control to the workflow engine. With the dependencies for wfid 2 now satisfied, the generated code for that workflow id executes:

```cpp
switch (wfid) {
    case 2U: {
        ctx->executeGraph("graph1",false,0,NULL);
        ctx->setResultString(0,1U,5U,"Done!");
    }
    break;
}
```

Most of the work for this workflow item involves executing graph1 (by calling back into ECL Agent/ROXIE). However, the code also directly sets another result. This is fairly typical - the code inside MyProcess::perform often combines evaluating scalar results, executing graphs, and calling functions that cannot (currently) be called inside a graph (for example, those involving Superfile transactions).

Once all of the required workflow items are executed, the workunit is
marked as completed. Alternatively, if there are workflow items that are
waiting to be triggered by an event, the workunit will be passed to the
scheduler, which will keep monitoring for events.

Note that most items in the xml workflow begin in the state WFStateNull.
This means that it is valid to execute them, but they haven't been
executed yet. Typically, only a few items begin with the state
WFStateReqd.

There are various specialized types of workflow items - e.g., sequential,
wait, independent, but they all follow the same basic approach of
executing dependencies and then executing that particular item.

Most of the interesting work in an ECL query is done within a graph. The
call ctx-\>executeGraph will either execute the graph locally (in the
case of hthor and roxie), or add the workunit onto a queue (for Thor).
Whichever happens, control will pass to that engine.

## Specialized Workflow Items

Each item mode/type can affect the dependency structure of the workflow:

-   Sequential/Ordered

    The workflow structure for sequential and ordered is the same. An
    item is made to contain all of the actions in the statement. This is
    achieved by making each action a dependency of this item. The
    dependencies, and consequently the actions, must be executed in
    order. An extra item is always inserted before each dependency. This
    means that if other statements reference the same dependency, it
    will only be performed once.

-   Persist

    When the persist workflow service is used, two items are created.
    One item contains the graphs that perform the expression defined in
    ECL. It also stores the wfid for the second item. The second item is
    used to determine whether the persist is up to date.

-   Condition (IF)

    The IF function has either 2 or 3 arguments: the expression, the
    trueresult, and sometimes the falseresult. For each argument, a
    workflow item is created. These items are stored as dependencies to
    the condition, in the order stated above.

-   Contingency (SUCCESS/FAILURE)

    When a contingency clause is defined for an attribute, the attribute
    item stores the wfid of the contingency. If both success and failure
    are used, then the item will store the wfid of each contingency. The
    contingency is composed of items, just like the larger query.

-   Recovery

    When a workflow item fails, if it has a recovery clause, the item
    will be re-executed. The clause contains actions defined by the
    programmer to remedy the problem. This clause is stored differently
    to SUCCESS/FAILURE, in that the recovery clause is a dependency of
    the failed item. In order to stop the recovery clause from being
    executed like the other dependencies, it is marked with WFStateSkip.

-   Independent

    This specifier is used when a programmer wants to common up code for
    the query. It prevents the same piece of code from being executed
    twice in different contexts. To achieve this, an extra item is
    inserted between the expression and whichever items depend on it.
    This means that although the attribute can be referenced several
    times, it will only be executed once.

### Graph Execution

All the engines (roxie, hThor, Thor) execute graphs in a very similar
way. The main differences are that hThor and Thor execute a sub graph at
a time, while roxie executes a complete graph as one. Roxie is also
optimized to minimize the overhead of executing a query - since the same
query tends to be run multiple times. This means that roxie creates a
graph of factory objects and those are then used to create the
activities. The core details are the same for each of them though.

#### Details of the graph structure

First, a recap of the structure of the graph together with the full xml
for the graph definition in our example:

```xml
<Graph name="graph1" type="activities">
    <xgmml>
        <graph wfid="2">
            <node id="1">
                <att>
                    <graph>
                        <att name="rootGraph" value="1"/>
                        <edge id="2_0" source="2" target="3"/>
                        <node id="2" label="Index Read&#10;&apos;names&apos;">
                            <att name="definition" value="workuniteg1.ecl(3,1)"/>
                            <att name="name" value="results"/>
                            <att name="_kind" value="77"/>
                            <att name="ecl" value="INDEX({ string40 name, string80 address }, &apos;names&apos;, fileposition(false));&#10;FILTER(KEYED(name = STORED(&apos;searchname&apos;)));&#10;"/>
                            <att name="recordSize" value="120"/>
                            <att name="predictedCount" value="0..?[disk]"/>
                            <att name="_fileName" value="names"/>
                        </node>
                        <node id="3" label="Output&#10;Result #1">
                            <att name="definition" value="workuniteg1.ecl(4,1)"/>
                            <att name="_kind" value="16"/>
                            <att name="ecl" value="OUTPUT(..., workunit);&#10;"/>
                            <att name="recordSize" value="120"/>
                        </node>
                    </graph>
                </att>
            </node>
        </graph>
    </xgmml>
</Graph>
```

Each graph (e.g., graph1) consists of 1 or more subgraphs (in the example
above, node id=1). Each of those subgraphs contains 1 or more activities
(node id=2, node id=3).

The xml for each activity might contain the following information:

-   A unique id (e.g., id="2").
-   The "kind" of the activity, e.g., \<att name="\_kind"
    value="77"/\>. The value is an item from the enum ThorActivityKind
    in eclhelper.hpp.
-   The ECL that created the activity. e.g., \<att name="ecl"
    value="\..."\>
-   The identifier of the ecl definition. e.g., \<att name="name"
    value="results"/\>
-   Location (e.g., file, line number, column) of the original ECL. e.g.,
    \<att name="definition" value="workuniteg1.ecl(3,1)"/\>
-   Meta information the code generator has deduced about the activity
    output. Examples include the record size, expected number of rows,
    sort order etc. e.g., \<att name="recordSize" value="120"/\>
-   Hints, which are used for fine control of options for a particular
    activity (e.g,, the number of threads to use while sorting).
-   Record counts and stats once the job has executed. (These are
    logically associated with the activities in the graph, but stored
    separately.)

Graphs also contain edges that can take one of 3 forms:

Edges within graphs

:   These are used to indicate how the activities are connected. The
    source activity is used as the input to the target activity. These
    edges have the following format:

        <edge id="<source-activity-id>_<output-count>" source="<source-activity-id>" target="<target-activity-id">

    There is only one edge in our example workunit: \<edge id="2\_0"
    source="2" target="3"/\>.

Edges between graphs

:   These are used to indicate direct dependencies between activities.
    For instance there will be an edge connecting the activity that
    writes a spill file to the activity that reads it. These edges have
    the following format:

        <edge id="<source-activity-id>_<target-activity-id>" source="<source-subgraph-id>" target="<target-subgraph-id>"
           <att name="_sourceActivity" value="<source-activity-id>"/>
           <att name="_targetActivity" value="<target-activity-id>"/>
        </edge>

    Roxie often optimizes spilled datasets and treats these edges as
    equivalent to the edges between activities.

Other dependencies.

:   These are similar to the edges between graphs, but they are used for
    values that are used within an activity. For instance one part of
    the graph may calculate the maximum value of a column, and another
    activity may filter out all records that do not match that maximum
    value. The format is the same as the edges between graphs except
    that the edge contains the following attribute:

        <att name="_dependsOn" value="1"/>

Each activity in a graph also has a corresponding helper class instance
in the generated code. (The name of the class is "cAc" followed by the
activity number, and the exported factory method is "fAc" followed by
the activity number.) Each helper class implements a specialized
interface (derived from IHThorArg) - the particular interface is
determined by the value of the "\_kind" attribute for the activity.

The contents of file rtl/include/eclhelper.hpp is key to understanding
how the generated code relates to the activities. Each kind of activity
requires a helper class that implements a specific interface. The
helpers allow the engine to tailor the generalized activity
implementation to the particular instance - e.g., for a filter
activity whether a row should be included or excluded. The appendix at
the end of this document contains some further information about this
file.

The classes in the generated workunits are normally derived from base
implementations of those interfaces (which are implemented in
rtl/include/eclhelper\_base.hpp). This reduces the size of the generated
code by providing default implementations for various functions.

For instance the helper for the index read (activity 2) is defined as:

```cpp
struct cAc2 : public CThorIndexReadArg {
    virtual unsigned getFormatCrc() {
        return 470622073U;
    }
    virtual bool getIndexLayout(size32_t & __lenResult, void * & __result) { getLayout5(__lenResult, __result, ctx); return true; }
    virtual IOutputMetaData * queryDiskRecordSize() { return &mx1; }
    virtual IOutputMetaData * queryOutputMeta() { return &mx1; }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) {
        ctx = _ctx;
        ctx->getResultString(v2,v1.refstr(),"searchname",4294967295U);
    }
    rtlDataAttr v1;
    unsigned v2;
    virtual const char * getFileName() {
        return "names";
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) {
        Owned<IStringSet> set3;
        set3.setown(createRtlStringSet(40));
        char v4[40];
        rtlStrToStr(40U,v4,v2,v1.getstr());
        if (rtlCompareStrStr(v2,v1.getstr(),40U,v4) == 0) {
            set3->addRange(v4,v4);
        }
        irc->append(createKeySegmentMonitor(false, set3.getClear(), 0, 40));
        irc->append(createWildKeySegmentMonitor(40, 80));
    }
    virtual size32_t transform(ARowBuilder & crSelf, const void * _left) {
        crSelf.getSelf();
        unsigned char * left = (unsigned char *)_left;
        memcpy(crSelf.row() + 0U,left + 0U,120U);
        return 120U;
    }
};
```

Some of the methods to highlight are:

a)  onCreate() - common to all activities. It is called by the engine
    when the helper is first created, and allows the helper to cache
    information that does not change - in this case the name that is
    being searched for.
b)  getFileName() - determines the name of the index being read.
c)  createSegmentMonitors() - defines which columns to filter, and which
    values to match against.
d)  transform() - create the record to return from the activity. It
    controls which columns should be included from the index row in the
    output. (In this case all.)

#### Executing the graph

To execute a graph, the engine walks the activities in the graph xml and
creates, in memory, a graph of implementation activities.

For each activity, the name of the helper factory is calculated from the
activity number (e.g., fAc2 for activity 2). That function is imported
from the loaded dll, and then called to create an instance of the
generated helper class - in this case cAc2.

The engine then creates an instance of the class for implementing the
activity, and passes the previously created helper object to the
constructor. The engine uses the \_kind attribute in the graph to
determine which activity class should be used. e.g., In the example above
activity 2 has a \_kind of 77, which corresponds to TAKindexread. For an
index-read activity roxie will create an instance of
CRoxieServerIndexReadActivity. (The generated helper that is passed to
the activity instance will implement IHThorIndexReadArg). The activity
implementations may also extract other information from the xml for the
activity - e.g., hints. Once all the activities are created the edge
information is used to link inputs activities to output activities and
add other dependencies.

Note: Any subgraph that is marked with `<att name="rootGraph" value="1"/>`
is a root subgraph. An activity within a subgraph that
has no outputs is called a 'sink' (and an activity without any inputs
is called a 'source').

Executing a graph involves executing all the root subgraphs that it
contains. All dependencies of the activities within the subgraph must be
executed before a subgraph is executed. To execute a subgraph, the
engine executes each of the sink activities on separate threads, and
then waits for each of those threads to complete. Each sink activity
lazily pulls input rows on demand from activities further up the graph,
processes them and returns when complete.

(If you examine the code you will find that this is a simplification.
The implementation for processing dependencies is more fine grained to
ensure IF datasets, OUPUT(,UPDATE) and other ECL constructs are executed
correctly.)

In our example the execution flows as follows:

1.  Only a single root subgraph, so need to execute that.
2.  The engine will execute activity 3 - the workunit-write activity
    (TAKworkunitwrite).
3.  The workunit-write activity will start its input.
4.  The index-read activity will call the helper functions to obtain the
    filename, resolve the index, and create the filter.
5.  The workunit-write activity requests a row from its input.
6.  The index-read finds the first row, and calls the helper's
    transform() method to create an output row.
7.  The workunit-write activity persists the row to a buffer (using the
    serializer provided by the IOutputMetaData interface implemented by
    the class mx1).
8.  Back to step 5, workunit-write reading a row from its input, until
    end of file is returned (notified as two consecutive NULL rows.
9.  Workunit-write commits the results and finishes.

The execution generally switches back and forth between the code in the
engines, and the members of the generated helper classes.

There are some other details of query execution that are worth
highlighting:

Child Queries

:   Some activities perform complicated operations on child datasets of
    the input rows. e.g., remove all duplicate people who are marked as
    living at this address. This will create a "child query" in the
    graph - i.e. a nested graph within a subgraph, which may be executed
    each time a new input row is processed by the containing activity.
    (The graph of activities for each child query is created at the same
    time as the parent activity. The activity instances are
    reinitialized and re-executed for each input row processed by the
    parent activity to minimize the create-time overhead.)

Other helper functions

:   The generated code contains other functions that are used to
    describe the meta information for the rows processed within the
    graph. e.g., the following class describes the output from the index
    read activity:

```cpp
struct mi1 : public CFixedOutputMetaData {
    inline mi1() : CFixedOutputMetaData(120) {}
    virtual const RtlTypeInfo * queryTypeInfo() const { return &ty1; }
} mx1;
```

    This represents a fixed size row that occupies 120 bytes. The object
    returned by the queryTypeInfo() function provides information about
    the types of the fields:

```cpp
const RtlStringTypeInfo ty2(0x4,40);
const RtlFieldStrInfo rf1("name",NULL,&ty2);
const RtlStringTypeInfo ty3(0x4,80);
const RtlFieldStrInfo rf2("address",NULL,&ty3);
const RtlFieldInfo * const tl4[] = { &rf1,&rf2, 0 };
const RtlRecordTypeInfo ty1(0xd,120,tl4);
```

    I.e. a string column, length 40 called "name", followed by a
    string column, length 80 called "address". The interface
    IOutputMetaData in eclhelper.hpp is key to understanding how the
    rows are processed.

Inline dataset operations

:   The rule mentioned at the start - that the generated code does not
    contain any knowledge of how to perform a particular dataset
    operation - does have one notable exception. Some operations on
    child datasets are very simple to implement, and more efficient if
    they are implemented using inline C++ code. (The generated code is
    smaller, and they avoid the overhead of setting up a child graph.)
    Examples include filtering and aggregating column values from a
    child dataset.

The full code in the different engines is more complicated than the
simplified process outlined above, especially when it comes to executing
dependencies, but the broad outline is the same.

### Appendix

More information on the work done in the code generator to create the
workunit can be found in ecl/eclcc/DOCUMENTATION.rst.

The C++ code can be generated as a single C++ file or multiple files.
The system defaults to multiple C++ files, so that they can be compiled
in parallel (and to avoid problems some compilers have with very large
files). When multiple C++ files are generated the metadata classes and
workflow classes are generated in the main module, and the activities
are generated in the files suffixed with a number. It may be easier to
understand the generated code if it is in one place. In which case,
compile your query with the option -fspanMultipleCpp=0. Use
-fsaveCppTempFiles to ensure the C++ files are not deleted (the C++
files will appear as helpers in the workunit details).

#### Key types and interfaces from eclhelper.hpp

IEclProcess

:   The interface that is used by the workflow engine to execute the
    different workflow items in the generated code.

ThorActivityKind

:   This enumeration contains one entry for each activity supported by
    the engines.

ICodeContext

:   This interface is implemented by the engine, and provides a
    mechanism for the generated code to call back into the engine. For
    example resolveChildQuery() is used to obtain a reference to a child
    query that can then be executed later.

IOutputMetaData

:   This interface is used to describe any meta data associated with the
    data being processed by the queries.

IHThorArg

:   The base interface for defining information about an activity. Each
    activity has an associated interface that is derived from this
    interface. e.g., each instance of the sort activity will have a
    helper class implementing IHThorSortArg in the generated query.
    There is normally a corresponding base class for each interface in
    eclhelper\_base.hpp that is used by the generated code e.g.,
    CThorSortArg.

ARowBuilder

:   This abstract base class is used by the transform functions to
    reserve memory for the rows that are created.

IEngineRowAllocator

:   Used by the generated code to allocate rows and rowsets. Can also be
    used to release rows (or call the global function rtlReleaseRow()).

IGlobalCodeContext

:   Provides access to functions that cannot be called inside a graph -
    i.e. can only be called from the global workflow code. Most
    functions are related to the internal implementation of particular
    workflow item types (e.g., persists).

#### Glossary

activity

:   An activity is the basic unit of dataset processing implemented by
    the engines. Each activity corresponds to a node in the thor
    execution graph. Instances of the activities are connected together
    to create the graph.

dll

:   A dynamically loaded library. These correspond to shared objects in
    Linux (extension '.so'), dynamic libraries in Max OS X
    ('.dylib'), and dynamic link libraries in windows ('.dll').

superfile

:   A composite file which allows a collection of files to be treated as
    a single compound file.

?What else should go here?

#### Full text of the workunit XML

The XML for a workunit can be viewed on the XML tag in eclwatch, or
generated by compiling the ECL using the -wu option with eclcc.
Alternatively eclcc -b -S can be used to generate the XML and the C++ at
the same time (the output filenames are derived from the input name).

```xml
<W_LOCAL buildVersion="internal_5.3.0-closedown0"
    cloneable="1"
    codeVersion="157"
    eclVersion="5.3.0"
    hash="2344844820"
    state="completed"
    xmlns:xsi="http://www.w3.org/1999/XMLSchema-instance">
    <Debug>
        <addtimingtoworkunit>0</addtimingtoworkunit>
        <debugnlp>1</debugnlp>
        <expandpersistinputdependencies>1</expandpersistinputdependencies>
        <forcegenerate>1</forcegenerate>
        <noterecordsizeingraph>1</noterecordsizeingraph>
        <regressiontest>1</regressiontest>
        <showmetaingraph>1</showmetaingraph>
        <showrecordcountingraph>1</showrecordcountingraph>
        <spanmultiplecpp>0</spanmultiplecpp>
        <targetclustertype>hthor</targetclustertype>
    </Debug>
    <Graphs>
        <Graph name="graph1" type="activities">
            <xgmml>
                <graph wfid="2">
                    <node id="1">
                        <att>
                            <graph>
                                <att name="rootGraph" value="1" />
                                <edge id="2_0" source="2" target="3" />
                                <node id="2" label="Index Read&#10;&apos;names&apos;">
                                    <att name="definition" value="workuniteg1.ecl(3,1)" />
                                    <att name="name" value="results" />
                                    <att name="_kind" value="77" />
                                    <att name="ecl"
                                        value="INDEX({ string40 name, string80 address }, &apos;names&apos;, fileposition(false));&#10;FILTER(KEYED(name = STORED(&apos;searchname&apos;)));&#10;" />
                                    <att name="recordSize" value="120" />
                                    <att name="predictedCount" value="0..?[disk]" />
                                    <att name="_fileName" value="names" />
                                </node>
                                <node id="3" label="Output&#10;Result #1">
                                    <att name="definition" value="workuniteg1.ecl(4,1)" />
                                    <att name="_kind" value="16" />
                                    <att name="ecl" value="OUTPUT(..., workunit);&#10;" />
                                    <att name="recordSize" value="120" />
                                </node>
                            </graph>
                        </att>
                    </node>
                </graph>
            </xgmml>
        </Graph>
    </Graphs>
    <Query fetchEntire="1">
        <Associated>
            <File desc="workuniteg1.ecl.cpp"
                filename="c:\regressout\workuniteg1.ecl.cpp"
                ip="10.121.159.73"
                type="cpp" />
        </Associated>
    </Query>
    <Results>
        <Result isScalar="0"
            name="Result 1"
            recordSizeEntry="mf1"
            rowLimit="-1"
            sequence="0">
            <SchemaRaw xsi:type="SOAP-ENC:base64">
                name&#xe000;&#xe004;(&#xe000;&#xe000;&#xe000;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;address&#xe000;&#xe004;P&#xe000;&#xe000;&#xe000;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;&#xe000;&#xe018;%&#xe000;&#xe000;&#xe000;{
                string40 name, string80 address };&#10; </SchemaRaw>
        </Result>
        <Result name="Result 2" sequence="1">
            <SchemaRaw xsi:type="SOAP-ENC:base64">
                Result_2&#xe000;&#xe004;&#241;&#255;&#255;&#255;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;&#xe000;&#xe018;&#xe000;&#xe000;&#xe000;&#xe000;
            </SchemaRaw>
        </Result>
    </Results>
    <Statistics>
        <Statistic c="eclcc"
            count="1"
            creator="eclcc"
            kind="SizePeakMemory"
            s="compile"
            scope=">compile"
            ts="1428933081084000"
            unit="sz"
            value="27885568" />
    </Statistics>
    <Variables>
        <Variable name="searchname">
            <SchemaRaw xsi:type="SOAP-ENC:base64">
                searchname&#xe000;&#xe004;&#241;&#255;&#255;&#255;&#xe001;ascii&#xe000;&#xe001;ascii&#xe000;&#xe000;&#xe018;&#xe000;&#xe000;&#xe000;&#xe000;
            </SchemaRaw>
        </Variable>
    </Variables>
    <Workflow>
        <Item mode="normal"
            state="null"
            type="normal"
            wfid="1" />
        <Item mode="normal"
            state="reqd"
            type="normal"
            wfid="2">
            <Dependency wfid="1" />
            <Schedule />
        </Item>
    </Workflow>
</W_LOCAL>
```

#### Full contents of the generated C++ (as a single file)

```cpp
/* Template for generating thor/hthor/roxie output */
#include "eclinclude4.hpp"
#include "eclrtl.hpp"
#include "rtlkey.hpp"

extern RTL_API void rtlStrToStr(size32_t lenTgt,void * tgt,size32_t lenSrc,const void * src);
extern RTL_API int rtlCompareStrStr(size32_t lenL,const char * l,size32_t lenR,const char * r);


const RtlStringTypeInfo ty2(0x4,40);
const RtlFieldStrInfo rf1("name",NULL,&ty2);
const RtlStringTypeInfo ty3(0x4,80);
const RtlFieldStrInfo rf2("address",NULL,&ty3);
const RtlFieldInfo * const tl4[] = { &rf1,&rf2, 0 };
const RtlRecordTypeInfo ty1(0xd,120,tl4);
void getLayout5(size32_t & __lenResult, void * & __result, IResourceContext * ctx) {
    rtlStrToDataX(__lenResult,__result,87U,"\000R\000\000\000\001x\000\000\000\002\000\000\000\003\004\000\000\000name\004(\000\000\000\001ascii\000\001ascii\000\000\000\000\000\000\003\007\000\000\000address\004P\000\000\000\001ascii\000\001ascii\000\000\000\000\000\000\002\000\000\000");
}
struct mi1 : public CFixedOutputMetaData {
    inline mi1() : CFixedOutputMetaData(120) {}
    virtual const RtlTypeInfo * queryTypeInfo() const { return &ty1; }
} mx1;
extern "C" ECL_API IOutputMetaData * mf1() { mx1.Link(); return &mx1; }

struct cAc2 : public CThorIndexReadArg {
    virtual unsigned getFormatCrc() {
        return 470622073U;
    }
    virtual bool getIndexLayout(size32_t & __lenResult, void * & __result) { getLayout5(__lenResult, __result, ctx); return true; }
    virtual IOutputMetaData * queryDiskRecordSize() { return &mx1; }
    virtual IOutputMetaData * queryOutputMeta() { return &mx1; }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) {
        ctx = _ctx;
        ctx->getResultString(v2,v1.refstr(),"searchname",4294967295U);
    }
    rtlDataAttr v1;
    unsigned v2;
    virtual const char * getFileName() {
        return "names";
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) {
        Owned<IStringSet> set3;
        set3.setown(createRtlStringSet(40));
        char v4[40];
        rtlStrToStr(40U,v4,v2,v1.getstr());
        if (rtlCompareStrStr(v2,v1.getstr(),40U,v4) == 0) {
            set3->addRange(v4,v4);
        }
        irc->append(createKeySegmentMonitor(false, set3.getClear(), 0, 40));
        irc->append(createWildKeySegmentMonitor(40, 80));
    }
    virtual size32_t transform(ARowBuilder & crSelf, const void * _left) {
        crSelf.getSelf();
        unsigned char * left = (unsigned char *)_left;
        memcpy(crSelf.row() + 0U,left + 0U,120U);
        return 120U;
    }
};
extern "C" ECL_API IHThorArg * fAc2() { return new cAc2; }
struct cAc3 : public CThorWorkUnitWriteArg {
    virtual int getSequence() { return 0; }
    virtual IOutputMetaData * queryOutputMeta() { return &mx1; }
    virtual void serializeXml(const byte * self, IXmlWriter & out) {
        mx1.toXML(self, out);
    }
};
extern "C" ECL_API IHThorArg * fAc3() { return new cAc3; }


struct MyEclProcess : public EclProcess {
    virtual unsigned getActivityVersion() const { return ACTIVITY_INTERFACE_VERSION; }
    virtual int perform(IGlobalCodeContext * gctx, unsigned wfid) {
        ICodeContext * ctx;
        ctx = gctx->queryCodeContext();
        switch (wfid) {
            case 1U:
                if (!gctx->isResult("searchname",4294967295U)) {
                    ctx->setResultString("searchname",4294967295U,5U,"Smith");
                }
                break;
            case 2U: {
                ctx->executeGraph("graph1",false,0,NULL);
                ctx->setResultString(0,1U,5U,"Done!");
            }
            break;
        }
        return 2U;
    }
};


extern "C" ECL_API IEclProcess* createProcess()
{

    return new MyEclProcess;
}
```

## Summary

Workunits are the foundation of query execution in the HPCC Systems® platform. Understanding their structure helps you:

- **Debug query execution issues** by examining workflow, graphs, and activity helpers
- **Optimize performance** by understanding how engines interact with generated code
- **Develop platform components** that process or analyze query execution

### Key Concepts to Remember

- **Separation of concerns**: Workunits contain the "what to do" (metadata), while engines implement the "how to do it" (algorithms)
- **Workflow control**: Manages dependencies and execution order of query components
- **Graph execution**: Activities are connected in graphs that engines execute
- **Helper classes**: Generated code provides specific behavior for generic engine activities

For more information about workunit generation, see `ecl/eclcc/DOCUMENTATION.rst`. To better understand generated code, compile with `-fspanMultipleCpp=0` and `-fsaveCppTempFiles` to keep all code in a single readable file.
