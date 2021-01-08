=================
Metrics Framework
=================

************
Introduction
************

The metrics framework provides a set of classes and templates for implementing a metrics collection strategy
in the platform.

The framework consists of three main areas. Each is responsible for a portion of the collection and reporting
operations. These areas are

* Metrics
* Sinks
* The Metrics Reporter

*******
Metrics
*******

Metrics represent the base value that a component must track. The framework defines a number of metric
classes for use by components. Each metric class encapsulates all of the necessary logic to handle the
following:

* Updating the value being tracked
* Synchronization between updating and collecting
* Providing measurements to the framework

Components are only responsible for creating metric objects, instrumenting the component to update
each metric, and adding each metric to the *Metrics Reporter*. The framework handles all remaining
tasks related to measurement collection and reporting. Components are independent of the collection
system(s) in use.

Please see the section on *Instrumenting a Component* for more information on component requirements.

Metrics and Measurements
------------------------

There is an important distinction between a metric and the measurements it provides. A *metric* is
the low level value defined by the component that tracks some state in the component. An example is
the number of requests processed. A *measurement* represents a view of the metric.
A *metric* may provide multiple measurements. For example, a component defines a metric to count the
number of times some event occurs. The metric, however, provides different measurements of the
event. For example it may provide the raw count, the number of events since the last time it was
collected, and the rate of events per second since the last read. The purpose for providing
different measurements from the base raw value is to support native value types defined by different collection
systems. Each metric defines its supported measurements.


Available Metrics
-----------------

The framework provides the following metrics.

* Counter
* Gauge

Counter Metric
==============

The *Counter* metric defines a monotonically increasing value used to count the occurrences of a component
defined event. The metric defines the following available measurements:
* count - The current metric value (default measurement)
* resetting_count - The count since the last collection
* rate - The rate in events per second since the last collection

Gauge Metric
============

A *Gauge* metric is used to track the instantaneous value of an internal component state. An example is
the size of a queue. Because the type of internal state varies, the gauge metric is a templated class.
A gauge only supports a single measurement, its current value.

*****
Sinks
*****

The framework defines a sink interface to support the different requirements of a collection systems.
Examples of collection systems are Prometheus, Data Dog, and Elasticsearch. Each has different requirements
for how and when measurements are ingested. The following are examples of different collection system
requirements:

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
requiring changes to a component. Therefore, components are independent of the collection system(s) in use.

Please see the sink interface description for more information.

****************
Metrics Reporter
****************

The *Metrics Reporter* is the framework central control object providing an opaque seamless connection
between configured sinks and component metrics. It handles

* Metrics reporting configuration
* Loading and configuring sinks
* Managing component created metrics
* Collecting measurements
* Reporting

The component is required to instantiate a reporter object and keep it alive for the duration of
the life of the component, or rather while the component is reporting metrics.


It has two main purposes. First, it handles configuration by processing the platform metrics configuration.
This includes loading configured sinks. Second it handles collection and reporting as a service to
each sink.

Each component is required to instantiate a single metrics reporter object that must
live for the duration of metrics collection.

*************************
Instrumenting a Component
*************************

Instrumenting a component follows these general steps. First and foremost, however, the component owner
must determine what to collect. A separate section below gives some ideas on the subject.

Once the metrics have been determined, the following basic steps outline how to instrument the component.

* Instantiate a metrics reporter object and pass it platform and component configuration information

::

    #include "metrics.hpp"

    using namespace hpccMetrics;

    //
    // Create a reporter
    MetricsReporter metricsReporter;

    //
    // Initialize metrics configuration
    IPropertyTree *pGlobalMetricsTree = queryGlobalConfig->getPropTree("config/metrics");
    IPropertyTree *pComponentMetricsTree = queryComponentConfig->getPropTree("roxie/metrics");
    metricsReporter.init(pGlobalMetricsTree, pComponentMetricTree);

* Instantiate metric objects for each metric the component is to collect and add them to the reporter

::

    //
    // Create counter and unsigned gauge metrics
    std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("requests", "The number of requests");
    std::shared_ptr<GaugeMetric<unsigned>> pQueueSize = std::make_shared<GaugeMetric<unsigned>>("queuesize", "The number of waiting requests");

    metricsReporter.addMetric(pCounter);
    metricsReporter.addMetric(pQueueSize);

* Instrument component code with updates to each metric when a relevant event occurs.

::

    //
    // Update the count
    pCounter->inc(1);

    //
    // Update adding a queue element
    pQueueSize->inc(1);

    //
    // Update element removed from the queue
    pQueueSize->dec(1);

* Tell the framework to start collecting. When complete, stop collecting

::

    //
    // Start collecting (framework handles the rest)
    metricsReporter.startCollecting();

    //
    // Stop collecting
    metricsReporter.stopCollecting();

When *startCollecting* is called, each registered sink begins is specific collection process. For some sinks
it may be a periodic timer that triggers a collection. Others may listen on a port for an external trigger.
The key takeaway is that the sink is responsible for triggering a collection.


***************************
Determining what to collect
***************************

This is perhaps the most important step when instrumenting a component for metrics collection. Metrics must
useful and actionable. If not, then it should not be collected. Metrics should follow one of the USE or RED
principals and be useful in an SRE (System Reliability Engineering) environment. The USE and RED principals
are beyond the scope of this document. There is ample literature avaiable with a simple search to learn
what these are.

Metrics should not be confused with logging. Metrics are simple, usually scalar numeric values that are
collected and stored for aggregate analysis to drive actionable results. For example, the number of requests
per second may drive a component scaling decision. Conversely, logs provide the deep drill down information
necessary to analyze system anomalies.
