# Metrics Framework Design

## Introduction

This document describes the design of a metrics framework that allows
HPCC Systems components to implement a metric collection strategy.
Metrics provide the following functionality:

-   Alerts and monitoring

    An important DevOps function is to monitor the cluster and providing
    alerts when problems are detected. Aggregated metric values from
    multiple sources provide the necessary data to build a complete
    picture of cluster health that drives monitoring and alerts.

-   Scaling

    As described above, aggregated metric data is also used to
    dynamically respond to changing cluster demands and load. Metrics
    provide the monitoring capability to react and take action

-   Fault diagnosis and resource monitoring

    Metrics provide historical data useful in diagnosing problems by
    profiling how demand and usage patterns may change prior to a fault.
    Predictive analysis can also be applied.

-   Analysis of jobs/workunits and profiling

    With proper instrumentation, a robust dynamic metric strategy can
    track workunit processing. Internal problems with queries should be
    diagnosed from deep drill down logging.

The document consists of several sections in order to provide
requirements as well as the design of framework components.

### Definitions

Some definitions are useful.

Metric

:   A measurement defined by a component that represents an internal
    state that is useful in a system reliability engineering function.
    In the context of the framework, a metric is an object representing
    the above.

Metric Value

:   The current value of a metric.

Metric Updating

:   The component task of updating metric state.

Collection

:   A framework process of selecting relevant metrics based on
    configuration and then retrieving their values.

Reporting

:   A framework process of converting values obtained during a
    collection into a format suitable for ingestion by a collection
    system.

Trigger

:   What causes the collection of metric values.

Collection System

:   The store for metric values generated during the reporting framework
    process.

## Use Scenarios

This section describes how components expect to use the framework. It is
not a complete list of all requirements but rather a sample.

### Roxie

Roxie desires to keep a count of many different internal values. Some
examples are

-   Disk type operations such as seeks and reads

-   Execution totals

    Need to track items such as total numbers of items such as success
    and failures as well as breaking some counts into individual
    reasons. For example, failures may need be categorized such as as

    -   Busy
    -   Timeout
    -   Bad input

    Or even by priority (high, low, sla, etc.)

-   Current operational levels such as the length of internal queues

-   The latency of operations such as queue results, agent responses,
    and gateway responses

Roxie also has the need to track internal memory usage beyond the
pod/system level capabilities. Tracking the state of its large fixed
memory pool is necessary.

The Roxie buddy system also must track how often and who is completing
requests. The "I Beat You To It" set of metrics must be collected and
exposed in order to detect pending node failure. While specific action
on these counts is not known up front, it appears clear that these
values are useful and should be collected.

There does not appear to be a need for creating and destroying metrics
dynamically. The set of metrics is most likely to be created at startup
and remain active through the life of the Roxie. If, however, stats
collection seeps into the metrics framework, dynamic creation and
destruction of stats metrics is a likely requirement.

### ESP

There are some interesting decisions with respect to ESP and collection
of metrics. Different applications within ESP present different use
cases for collection. Ownership of a given task drives some of these use
cases. Take workunit queues. If ownership of the task, with respect to
metrics, is WsWorkunits, then use cases are centric to that component.
However, if agents listening on the queue are to report metrics, then a
different set of use cases emerge. It is clear that additional work is
needed to generate clear ownership of metrics gathered by ESP and/or the
tasks it performs.

ESP needs to report the *activeTransactions* value from the TxSummary
class(es). This gives an indication of how busy the ESP is in terms of
client requests.

Direct measurement of response time in requests may not be useful since
the type of request causes different execution paths within ESP that are
expected to take widely varying amounts of time. Creation of metrics for
each method is not recommended. However, two possible solutions are to
a) create a metric for request types, or b) use a histogram to measure
response time ranges. Another option mentioned redefines the meaning of
a bucket in a histogram. Instead of a numeric distribution, each bucket
represents a unique subtask within an overall "metric" representing a
measured operation. This should be explored whether for operational or
developmental purposes.

For tracking specific queries and their health, the feeling is that
logging can accomplish this better than metrics since the list of
queries to monitor will vary between clusters. Additionally, operational
metrics solving the cases mentioned above will give a view into the
overall health of ESP which will affect the execution of queries.
Depending on actions taken by these metrics, scaling may solve overload
conditions to keep cluster responsiveness acceptable.

For Roxie a workunit operates as a service. Measuring service
performance using a histogram to capture response times as a
distribution may be appropriate. Extracting the 95th percentile of
response time may be useful as well.

There are currently no use cases requiring consistency between values of
different metrics.

At this time the only concrete metric identified is the number of
requests received. As the framework design progresses and ESP is
instrumented, the list will grow.

### Dali Use Cases

From information gathered, Dali plans to keep counts and rates for many
of the items it manages.

## Framework Design

This section covers the design and architecture of the framework. It
discusses the main areas of the design, its interactions, and an overall 
process model of how the framework operates.

The framework consists of three major areas: metrics, sinks, and the
glue logic. These areas work together with the platform and the
component to provide a reusable metrics collection framework.

Metrics represent the quantifiable component state measurements used to
track and assess the status of the component. Metrics are typically
scalar values that are easily aggregated by a collection system.
Aggregated values provide the necessary input to take component and
cluster actions such as horizontal scaling. The component is
responsible for creating metrics and instrumenting the code. The
framework provides the support for collecting and reporting the values.
Metrics provide the following:

-   Simple methods for the component to create and update the metric
-   Simple methods for the framework to retrieve metric value(s)
-   Handling of all synchronization between updating and retrieving
    metric values

In addition, the framework provides the support for retrieving values so
that the component does not participate in metric reporting. The
component simply creates required metrics, then instruments the
component to update the metric for state changes and/or events. For example,
the component may create a metric to count the total number of
requests received. Then, wherever the component receives a request, it increments
the count.

Sinks provide a pluggable interface to the framework that hides the specifics of
the collection system. This keeps both the framework and instrumented
components independent of the collection system. Examples of collection systems
are Prometheus, Datadog, and Elasticsearch.

Sinks have the following responsibilities:

-   Operate independently of other sinks in the system
-   Convert metric native values into collection system specific
    measurements and reports
-   Drive the collection and reporting processes

The third area of the framework is the glue logic, referred to as the
*MetricsFramework*. It manages the metrics system for the component and sink(s). It
provides the following:

-   Handles framework initialization
-   Loads sinks as required
-   Manages the list of metrics for components
-   Handles collection and reporting with a set of methods used by sinks

The framework provides a manager singleton object shared by all components in the same
address space. 

## Framework Implementation

The framework is implemented within jlib. The following sections
describe each area of the framework.

### Metrics

Components use metrics to measure their internal state. Metrics can
represent everything from the number of requests received to the average
length some value remains cached. Components are responsible for
creating and updating metrics for each measured state. The framework
provides a set of metrics designed to cover the majority of
component measurement requirements. 

To meet the requirement to manage metrics independent of the underlying
metric state, all metrics implement a common interface. All metrics then
add their specific methods to update and retrieve internal state.
Generally, the component uses the update method(s) to update state and
sinks use the retrieval methods to get current state when
reporting. The metric insures synchronized access.

For components that already have an implementation that tracks a measurement,
the framework provides a way to instantiate a custom metric. The custom
metric allows the component to leverage the existing implementation and
give the framework access to the metric value for collection and
reporting. Note that custom metrics only support simple scalar metrics
such as a counter or a gauge.

### Sinks

Sinks are responsible for two main functions: initiating a collection
and reporting measurements to the collection system. The *Metrics
Manager* provides the support to complete these functions.

The framework defines a sink interface to support the different
requirements of collection systems. Examples of collection systems are
Prometheus, Datadog, and Elasticsearch. Each has different requirements
for how and when measurements are ingested. The following are examples
of different collection system requirements:

-   Polled vs Periodic
-   Single measurement vs multiple reports
-   Report format (JSON, text, etc.)
-   Push vs Pull


The sink encapsulates all collection system requirements
providing a pluggable architecture that isolates components from collecting and
reporting. The framework can support multiple sinks concurrently, each
operating independently.

Instrumented components are not aware of the sink or sinks in use. Sinks
can be changed without requiring changes to a component, keeping
components independent of the collection system(s) in use.

### Metrics Manager

The MetricsManager class provides the common functions to bind
together the component, the metrics it creates, and the sinks to which
measurements are reported. It is responsible for the following:

-   Initialization of the framework
-   Managing the metrics created by components
-   Supporting collection and reporting as directed by configured sinks

### Metrics Implementations

The sections that follow discuss metric implementations.

#### Counter Metric

A counter metric is a monotonically increasing value that "counts" the
total occurrences of some event. Examples include the number of requests
received, or the number of cache misses. Once created, the component
instruments the code with updates to the count whenever appropriate.

#### Gauge Metric

A gauge metric is a continuously updated value representing the current
state of an interesting value in the component. For example, the amount
of memory used in an internal buffer, or the number of requests waiting
on a queue. A gauge metric may increase or decrease in value as needed.
Reading the value of a gauge is a stateless operation in that there are
no dependencies on the previous reading. The value returned is always
the current state.

Once created, the component updates the gauge as state changes. 
The metric provides methods to increase and decrease the value. 

#### Custom Metric

A custom metric is a class that allows a component to leverage existing
metrics. The component creates an instance of a custom metric (a
templated class) and passes a reference to the underlying metric value.
When collection is performed, the custom metric simply reads the value
of the metric using the reference provided during construction. The
component maintains full responsibility for updating the metric value as
the custom metric class provides no update methods. One minor drawback
is that the famework provides no synchronization for the metric value.

#### Histogram Metric

Defines a distribution of measurements into a predefined set of intervals
known as buckets. When created, the component must provide the bucket limits.
When recording a measurement, the histogram metric increments the count of
the relevant bucket by using the measurement against defined bucket limits.
Each bucket contains a monotonically increasing count of measurements falling
inside its range. Additionally, the metric maintains a count of measurements that
fall outside the maximum bucket limit. This is known as the "inf" bucket.

The sink is free to convert the bucket counts into a suitable format for
reporting.

#### Scaled Histogram Metric

A histogram metric that allows setting the bucket limit units in one
domain, but take measurements in another domain. For example, the bucket
limits may represent millisecond durations, yet it is more efficient to
use execution cycles to take the measurements. A scaled histogram
converts from the measurement domain (cycles) to the limit units
domain using a scale factor provided at initialization. All conversions
are encapsulated in the scaled histogram class such that no external
scaling is required by any consumer such as a sink.

## Configuration

This section discusses configuration. Since Helm charts are capable of
combining configuration data at a global level into a component's
specific configuration, The combined configuration takes the form as
shown below. Note that as the design progresses it is expected that
there will be additions.

```yaml
    component:
      metrics:
        sinks:
        - type: <sink_type>
          name: <sink name>
          settings:
            sink_setting1: sink_setting_value1
            sink_setting2: sink_setting_value2
```

Where:

metrics

:   Metrics configuration for the component

metrics.sinks

:   List of defined sinks

metrics.sinks\[\].type

:   The sink type. The type is substituted into the following
    pattern to determine the lib to load:
    libhpccmetrics\<type\>\<shared\_object\_extension\>

metrics.sinks\[\].name

:   A name for the sink.

metrics.sinks\[\].settings

:   A set of key/value pairs passed to the sink when initialized. It
    should contain information necessary for the operation of the sink.
    Nested YML (or XML) is supported. Example settings are the Prometheus server
    name, or the collection period for a periodic sink.

## Metric Naming

Metric names shall follow a convention as outlined in this section.
Because different collection systems have different requirements for how
metric value reports are generated, naming is split into two parts.

First, each metric is given a base name that describes what the
underlying value is. Second, metadata is assigned to each metric to
further qualify the value. For example, a set of metrics may count the
number of requests a component has received. Each metric would have the
same base name, but metadata would separate types of request (GET vs
POST), or disposition such as pass or fail.

The combination of base name and metadata provides a unique signature for 
each metric.

### Base Name

The following convention defines how metric names are formed:

-   Name consists of parts separated by a period (.)

-   Name must begin with a character and may not end with a period (.)

-   Only one period is allowed between parts

-   Only uppercase and lowercase letters and numbers are allowed in each part.

-   Each part shall use camel case (allows for compound names in each
    part). Note that a part, other than the first, may begin with a number.

-   Each name shall begin with a prefix representing the scope of the
    metric (For example: _esp_ or _dali_)

-   The remainder of the name shall follow these conventions:

    Gauges: \<scope\>.\<plural-noun\>.\<state\> Examples: esp.requests.waiting,
    esp.status.requests.waiting

    Counters: \<scope\>.\<plural-noun\>.\<past-tense-verb\>
    Examples: thor.requests.failed, esp.gateway.requests.queued

    Time: \<scope\>.\<singular-noun\>.\<state or active-verb\>.time Examples: 
    dali.request.blocked.time, dali.request.process.time

### Metadata

Metadata further qualifies a metric value. This allows metrics to have
the same name, but different scopes or categories. Generally, metadata
is only used to further qualify metrics that have the same base
name, but need further distinction. An example best describes a use case
for metadata. Consider a component that accepts HTTP requests, but
needs to track GET and POST requests separately. Instead of defining
metrics with names _post.requests.received_ and
_get.requests.received_, the component creates two metrics with the
base name _requests.received_ and attaches metadata describing the
request type. In this case. "post" for the former, and "get" for the latter.

Use of metadata allows aggregating both types of requests into a single
combined count of received requests while allowing a breakdown by type.

Metadata is represented as key/value pairs and is attached to the
metric by the component during metric creation. The sink is responsible
for converting metadata into useful information for the collection
system during reporting.

The *Component Instrumentation* section covers how metadata is added to
a metric.

The key for metadata is a string, a name, representing what the value is. The name
string must be unique within the metadata for a metric. Names do not have any restrictions,
but should be short and descriptive. 

The value is a string must consist of only uppercase and lowercase letters and numbers.
Generally the value should be a single word or number (such as a port number). The value 
should follow the camel case convention. 

## Component Instrumentation

In order to instrument a component for metrics using the framework, a
component must include the metrics header from jlib (*jmetrics.hpp*) and
add jlib as a dependent lib (if not already doing so).

The general steps for instrumentation are

1.  Determine what component state to measure. This requires deep knowledge of the 
    component.
2.  Create metric objects for each measured state and add each to the manager.
3.  Add updates for each metric throughout the component to track state
    changes and/or events.

The component may retrieve the metrics manager singleton object and manually create
and add metrics. To simplify the process of metric creation (2 above), the framework 
provides a set of register functions for each metric. Each creates a metric, registers
it, and returns a shared pointer to the metric (see below). Use of the register functions 
is the recommended approach and alleviates the component from needing a reference 
to the manager.

If the component desires to get a reference to the metrics manager singleton, the following
code is recommended:

```cpp
using namespace hpccMetrics;
MetricsManager &metricsManager = queryMetricsManager();
```

Metrics are wrapped by a standard C++ shared pointer. The component is
responsible for saving the pointer in order to maintain a reference to the metric. 
The framework keeps a weak pointer to each metric and thus does not maintain a reference. 

As mentioned above, the framework provides a set of register functions for each metric type. See
the header file for the complete list of register functions. 

The following is an example of creating and registering a counter metric. 

```cpp
std::shared_ptr<CounterMetric> pCounter = registerCounterMetric("metric.name", "description");
```

### Metric Creation
The method by which a component creates a metric depends on component implementation. The sections
that follow provide guidance on different methods of metric creation. Choose the method that
best matches component implementation.

Metrics are guaranteed to always be created. Problems only arise when registering. If there is a
failure during registration, the metric can still be used within the component to update state. 
However, the metric will not be reported during collection. Generally, registration failures are 
for one of the following reasons:
* Duplicate metric name (for purposes of unique metric naming, the combination of metric 
name and metadata must be unique)
* Invalid metric name or metadata value

#### During Module Initialization
If the component is a shared library whose code is not also shared with a main application, 
then the component should create metrics during module initialization. The following code shows how
to register a metric during module initialization:

```cpp
static std::shared_ptr<hpccMetrics::CounterMetric> pCount;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    pCount = hpccMetrics::registerCounterMetric("name", "description", SMeasureCount);
    return true;
}

MODULE_EXIT()
{
    pCount = nullptr;    // note this is optional
}
```

Note that the metric is not required to be a static variable. Other options include:
* A global variable
* A static class member variable

#### During Construction
If the metric is based on measuring state in a class that is created and destroyed multiple times,
then the metric should be a static variable in the class. The metric is created during the first
construction and then reused for subsequent constructions. The metric is destroyed when the class
static variables are destroyed. The following code shows how to register a metric once:

```cpp

//
// Allocate the class static variables
static std::once_flag ClassX::metricsInitialized;
static std::shared_ptr<hpccMetrics::CounterMetric> ClassX::pCount1;

class ClassX
{
public:
    ClassX();
    void method1();
    
private:
    static std::shared_ptr<hpccMetrics::CounterMetric> pCount1;
    static std::once_flag metricsInitialized;
}

ClassX::ClassX()
{
    std::call_once(metricsInitialized, []()
    {
        pCount1 = hpccMetrics::registerCounterMetric("metric1", "description");
    });    
}

void ClassX::method1()
{
    pCount1->inc(1);   // Update state
}

```

The static _once_flag_ and _call_once_ block ensure that the metric is
only created once. The first time the constructor is called, the lambda is executed. Subsequent
calls to the constructor do not execute the lambda. The static shared\_ptr for each metric maintain
references to each metric making them available throughout the class.

#### Static Creation
Static creation should be a last resort. If the above methods do not solve the problem of metric
allocation and registering, then static creation is the next best choice. The primary reason to
avoid static creation is that if there is a problem during creation, any log messages
or exceptions are lost. Debugging is more difficult in this case.

An example of static creation is shown below:

```cpp

//
// Allocate the class static variables
static std::shared_ptr<hpccMetrics::CounterMetric> pCount1 = hpccMetrics::registerCounterMetric("metric1", "description");

```

### Metric State Updates

Note in the examples above, only a single line of code is needed to update state. This is true for 
all metric types.

That's it! There are no component requirements related to collection or
reporting of metric values. That is handled by the framework and loaded
sinks.

### Custom Metrics
If your component is already tracking a metric, you may not need to convert it to 
a defined framework metric type. Instead, you can create a custom metric and 
pass a reference to the existing metric value. The metric value, however, must be
a scalar value that can cast to a 64bit unsigned integer (\_\_uint64). 

The following is an example of creating a custom metric (you must provide the metric type):

```cpp
    auto pCustomMetric = registerCustomMetric("customName", "description", metricType, value);
```

### Adding Metric Metadata

A component, depending on requirements, may attach metadata to further
qualify created metrics. Metadata takes the form of key value pairs.
The base metric class *MetricBase* constructor defines a parameter for a
vector of metadata. Metric subclasses also define metadata as a
constructor parameter, however an empty vector is the default. The
*IMetric* interface defines a method for retrieving the metadata.

Meta data is order dependent.

Below are two examples of registering a counter metric with metadata. The first
creates the vector and passes it as a parameter. The second constructs the vector in place.

```cpp
MetricMetaData metaData1{{"key1", "value1"}};
std::shared_ptr<CounterMetric> pCounter1 =
    registerCounterMetric("requests.completed", "description", SMeasureCount, metaData1);

std::shared_ptr<CounterMetric> pCounter2 =
    registerCounterMetric("requests.completed", "description", SMeasureCount, MetricMetaData{{"key1", "value2"}});
```

### Metric Units

Metric units are treated separately from the base name and metadata.
The reason is to allow the sink to translate based on collection system
requirements. The base framework provides a convenience method for
converting units into a string. However, the sink is free to do any
conversions, both actual units and the string representation, as needed.

Metric units are defined using a subset of the *StatisticMeasure*
enumeration values defined in **jstatscodes.h**. The current values are
used:

-   SMeasureTimeNs - A time measurement in nanoseconds
-   SMeasureCount - A count of events
-   SMeasureSize - Size in bytes
