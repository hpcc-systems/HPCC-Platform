# Optimizing Roxie Query Performance

Roxie provides robust functionality to stabilize performance and reduce variability in query response times, particularly when optimizations are applied behind the scenes. This guide explains how to configure and monitor Roxie for consistent query execution.

## Setting a Minimum Query Execution Time

To ensure consistent response times, you can define a **minimum query execution time** (in milliseconds) in the Roxie engine's configuration. This setting can also be overridden for specific queries as needed.

### Configuration Options

The following settings can be used to define minimum execution times for different priority levels:

- `defaultLowPriorityTimeMinimum`
- `defaultHighPriorityTimeMinimum`
- `defaultSLAPriorityTimeMinimum`

### Implementation Methods

#### For Bare-Metal Systems

Modify the Roxie Topology section in `environment.xml` using the Configuration Manager tool to set the desired values.

#### For Containerized Systems

Include the settings in an override YAML file for the Roxie cluster. This approach is ideal for containerized deployments.

#### In ECL Code

Specify the minimum execution time directly in your ECL code using the `#OPTION` directive:

```ecl
#OPTION('minTimeLimit', 500); // Sets minimum execution time to 500 milliseconds (0.5 seconds)
```

#### In the Request URL

Add the minimum time limit as a parameter in the request:

```xml
<soap:Envelope>
  <soap:Body>
    <exampleRequest xmlns="urn:hpccsystems:ecl:example">
      <_MinTimeLimit>500</_MinTimeLimit>
      <!-- Sets minimum execution time to 500 milliseconds (0.5 seconds) -->
      <acctno>1111111</acctno>
    </exampleRequest>
  </soap:Body>
</soap:Envelope>
```

## Monitoring Performance

When the minimum query execution time feature is enabled, Roxie provides two key timing metrics to help monitor performance:

- **TimeDelayed**: The artificial delay added to meet the minimum execution time.
- **TimeExecuted**: The actual execution time of the query, excluding any added delay.

If a query completes faster than the specified minimum time, Roxie will pause execution until the threshold is met, ensuring consistent response times.

---

By leveraging these configuration options and monitoring tools, you can optimize Roxie query performance and maintain predictable response times across your system.
