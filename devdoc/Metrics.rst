========================
Metrics Framework Design
========================

************
Introduction
************

This document describes the design of a metrics framework that allows HPCC Systems components to
implement a metric collection strategy. Metrics provide the following functionality:

* Alerts and monitoring

  An important DevOps function is to monitor the cluster and providing alerts when
  problems are detected. Aggregated metric values from multiple sources provide a
  the necessary data to build a complete picture of cluster health that drives
  monitoring and alerts.

* Scaling

  As described above, aggregated metric data is also used to dynamically respond to changing
  cluster demands and load. Metrics provide the monitoring capability to react and take
  action

* Fault diagnosis and resource monitoring

  Metrics provide historical data useful in diagnosing problems by profiling how demand and
  usage patterns may change prior to a fault. Predictive analysis can also be applied.

* Analysis of jobs/workunits and profiling

  With proper instrumentation, a robust dynamic metric strategy can track workunit
  processing. Internal problems with queries should be diagnosed from deep drill down logging.

The document consists of several sections in order to provide requirements as well as
the design of framework components.

Definitions
===============
Some definitions are useful.

Metric
  A measurement defined by a component that represents an internal state that is useful in a system
  reliability engineering function. In the context of the framework, a metric is an object representing
  the above.

Metric Value
  The current value of a metric. A corollary is a metric measurement which represents a view of a metric
  value that may be based on state.

Metric Updating
  The component task of updating metric state.

Collection
  A framework process of selecting relevant metrics based on configuration and then retrieving
  their values.

Reporting
  A framework process of converting values obtained during a collection into a format suitable for
  ingestion by a collection system.

Trigger
  What causes the collection of metric values.

Collection System
  The store for metric values generated during the reporting framework process.


*************
Use Scenarios
*************
This section describes how components expect to use the framework. It is not a complete list of all
requirements but rather a sample.


Roxie
=====
Roxie desires to keep a count of many different internal values. Some examples are

* Disk type operations such as seeks and reads
* Execution totals

  Need to track items such as total numbers of items such as success and failures
  as well as breaking some counts into individual reasons. For example, failures
  may need be categorized such as as

  * Busy
  * Timeout
  * Bad input

  Or even by priority (high, low, sla, etc.)

* Current operational levels such as the length of internal queues
* The latency of operations such as queue results, agent responses, and gateway responses

Roxie also has the need to track internal memory usage beyond the pod/system level capabilities.
Tracking the state of its large fixed memory pool is necessary.

The Roxie buddy system also must track how often and who is completing requests. The "I Beat You To It"
set of metrics must be collected and exposed in order to detect pending node failure. While specific
action on these counts is not known up front, it appears clear that these values are useful and should
be collected.

There does not appear to be a need for creating and destroying metrics dynamically. The set of metrics
is most likely to be created at startup and remain active through the life of the Roxie. If, however,
stats collection seeps into the metrics framework, dynamic creation and destruction of stats metrics is
a likely requirement.


ESP
===

There are some interesting decisions with respect to ESP and collection of metrics. Different
applications within ESP present different use cases for collection. Ownership of a given task drives
some of these use cases. Take workunit queues. If ownership of the task, with respect to metrics, is
WsWorkunits, then use cases are centric to that component. However, if agents listening on the queue
are to report metrics, then a different set of use cases emerge. It is clear that additional work is
needed to generate clear ownership of metrics gathered by ESP and/or the tasks it performs.

ESP needs to report the *activeTransactions* value from the TxSummary class(es). This gives an
indication of how busy the ESP is in terms of client requests.

Direct measurement of response time in requests may not be useful since the type of request causes
different execution paths within ESP that are expected to take widely varying amounts of time. Creation
of metrics for each method is not recommended. However, two possible solutions are to a) create a
metric for request types, or b) use a histogram to measure response time ranges. Another option
mentioned redefines the meaning of a bucket in a histogram. Instead of a numeric distribution,
each bucket represents a unique subtask within an overall "metric" representing a measured operation.
This should be explored whether for operational or developmental purposes.

For tracking specific queries and their health, the feeling is that logging can accomplish this better
than metrics since the list of queries to monitor will vary between clusters. Additionally, operational
metrics solving the cases mentioned above will give a view into the overall health of ESP which will
affect the execution of queries. Depending on actions taken by these metrics, scaling may solve
overload conditions to keep cluster responsiveness acceptable.

For Roxie a workunit operates as a service. Measuring service performance using a histogram to capture
response times as a distribution may be appropriate. Extracting the 95th percentile of response time
may be useful as well.

There are currently no use cases requiring consistency between values of different metrics.

At this time the only concrete metric identified is the number of requests received. As the framework
design progresses and ESP is instrumented, the list will grow.


Dali Use Cases
==============

From information gathered, Dali plans to keep counts and rates for many of the items it manages.


****************
Framework Design
****************

This section covers the design and architecture of the framework. It discusses the main areas of the
design, the interactions between each area, and an overall process model of how the framework operates.

The framework consists of three major areas: metrics, sinks, and the glue logic. These area work
together with the platform and the component to provide a reusable metrics collection function.

Metrics represent the quantifiable component state measurements used to track and assess the status
of the component and the overall cluster. Metrics are typically scalar values that are easily
aggregated by a collection system. Aggregated values provide the necessary input to take component
and cluster actions such as scaling up and down. The component is responsible for creating metrics
and instrumenting the code. The framework provides the support for collecting and reporting the
values. Metrics provide the following:

* Simple methods for the component to update the metric
* Simple methods for the framework to retrieve metric value(s)
* Handling of all synchronization between updating and retrieving metric values

In addition, the framework provides the support for retrieving values so that the component does not
participate in metric reporting. The component simply creates the metrics it needs, then instruments
the component to update the metric whenever its state changes. For example, the component may create
a metric that counts the total number of requests it has received. Then, wherever the component
receives a request, a corresponding update to the count is added. Nowhere in the component is any
code added to retrieve the count as that is handled by the framework.

Sinks provide a pluggable interface to hide the specifics of collection systems so that the metrics
framework is independent of those dependencies. Sinks:

* Operate independently of other sinks in the system
* Convert metric native values into collection system specific measurements and reports
* Drive the collection and reporting processes

The third area of the framework is the glue logic, referred to as the *MetricsReporter*. It manages
the metrics system for the component. It provides the following:

* Handles framework initialization
* Loads sinks as required
* Manages the list of metrics for the component
* Handles collection and reporting with a set of convenience methods used by sinks

The framework is designed to be instantiated into a component as part of its process and address space.
All objects instantiated as part of the framework are owned by the component and are not shareable with
any other component whether local or remote. Any coordination or consistency requirements that may
arise in the implementation of a sink shall be the sole responsibility of the sink.

************************
Framework Implementation
************************
This section describe the implementation of each area of the framework.

Metrics
=======
Components use metrics to measure their internal state. Metrics can represent everything from the
number of requests received to the average length some value remains cached. The point is that the
component is responsible for creating and updating the metric. The framework shall provide a set of
metrics designed to cover the majority of component measurement requirements. All metrics share a
common interface to allow the framework to manage them in a common way.

To meet the requirement to manage metrics independent of the underlying metric state, all metrics
inherit from a common interface. All metrics implement the interface and add their specific methods
to update and retrieve internal state. Generally the component uses the update method(s) to change
metric state whenever an event or other process dictates. The sink, described later, is generally
the consumer of the retrieval methods. Components create and update metrics and sinks retrieve and
consume the values. The metric is responsible for synchronizing access between update and retrieval.

Sinks
===============
The framework defines a sink interface to support the different requirements of a collection systems.
Examples of collection systems are Prometheus, Datadog, and Elasticsearch. Each has different
requirements for how and when measurements are ingested. The following are examples of different
collection system requirements:

* Polled vs Periodic
* Single measurement vs multiple reports
* Report format (JSON, text, etc.)
* Push vs Pull

Sinks are responsible for two main functions: initiating a collection and reporting
measurements to the collection system. The *Metrics Reporter* provides the support to complete
these functions.

The sink encapsulates all of the collection system requirements providing a pluggable architecture that
isolates components from these differences. The framework supports multiple sinks concurrently,
each operating independently.

Instrumented components are not aware of the sink or sinks in use. Sinks can be changed without
requiring changes to a component. Therefore, components are independent of the collection system(s)
in use.


Metrics Reporter
================

The metrics reporter class provides all of the common functions to bind together the component,
the metrics it creates, and the sinks to which measurements are reported. It is responsible for
the following:

* Initialization of the framework
* Managing the metrics created by the component
* Handling collection and reporting as directed by configured sinks


Metrics Implementations
=======================

The sections that follow discuss metric implementations.

Gauge Metric
------------
A gauge metric is a continuously updated value representing the current state of an interesting value
in the component. For example, the amount of memory used in an internal buffer, or the number of
requests waiting on a queue. A gauge metric may increase or decrease in value as needed. Reading the
value of a gauge is a stateless operation in that there are no dependencies on the previous reading.
The value returned shall always be the current state.

Once created, the component shall update the gauge anytime the state of what is measured is updated.
The metric shall provide methods to increase and decrease the value. The sink reads the value during
collection and reporting.


*************
Configuration
*************
This section discusses configuration. Since Helm charts are capable of combining configuration data
at a global level into a component's specific configuration, The combined configuration takes the
form as shown below. Note that as the design progresses it is expected that there will be additions.

::

  component:
    metrics:
      sinks:
        - name: <sink name>
          type: <sink_type>
          settings:
            sink_setting1: sink_setting_value1
            sink_setting2: sink_setting_value2
          metrics:
            - name: <metric_name>

Where (based on being a child of the current *component*):

metrics
    Metrics configuration for the component

metrics.sinks
    List of sinks defined for the component (may have been combined with global config)

metrics.sinks.name
    A name for the sink. Note this may not be needed, but can provide a way to combine global and
    component config based on value

metrics.sinks.type
    The type for the sink. The type is subsituted into the following pattern to determine the lib to load:
    libhpccmetrics<type><shared_object_extension>

metrics.sinks.settings
    A set of key/value pairs that passed to the sink when initialized. It should contain information
    necessary for the operation of the sink. Nested YML is supported. Example settings are the
    prometheus server name, or the collection period for a periodic sink.

metrics.sinks.metrics
    Optional list of component-defined metrics reported by the sink to the backend during collection
    and reporting. If no list if given, all component metrics are reported by default.


*************************
Component Instrumentation
*************************

This section describes component instrumentation. Will be filled in later.
