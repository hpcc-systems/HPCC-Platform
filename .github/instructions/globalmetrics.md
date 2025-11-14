# General comments about metrics

For all metrics and stats recorded by the system:

- **All costs are recorded in millionths of a dollar** (cost_type values)
- **All times are recorded in nanoseconds** (StatisticKind with SMeasureTimeNs)
- **All timestamps are microseconds since the start of the epoch** (StatisticKind with SMeasureTimestampUs)
- **All counts are simple integer values** (StatisticKind with SMeasureCount)
- **All sizes are in bytes** (StatisticKind with SMeasureSize)

# Global metrics

The system records global metrics for each component in Dali using the `recordGlobalMetrics()` global function. The metrics are recorded as a time series in buckets where the startTime and endTime record the start and end of the bucket in the form YYYYMMDDHH. The time range is inclusive.

## Function signatures

The system provides three overloads of recordGlobalMetrics():

```cpp
// From a statistics collection
void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions,
                        const CRuntimeStatisticCollection & stats, const StatisticsMapping * optMapping);

// From individual statistics (initializer lists)
void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions,
                        const std::initializer_list<StatisticKind> & stats, const std::initializer_list<stat_type> & values);

// From vectors
void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions,
                        const std::vector<StatisticKind> & stats, const std::vector<stat_type> & deltas);
```

Where:
- `category` - The type of metrics being recorded (e.g., "Queue", "testCategory")
- `MetricsDimensionList` - A vector of key-value pairs: `std::vector<std::pair<const char *, const char *>>`
- The statistics and values must be the same length and correspond positionally

## Dimension structure

Metrics are grouped by dimensions in a hierarchical structure. Common dimension patterns include:

### Component-level dimensions (user-independent metrics):
```cpp
{ {"component", "thor" }, { "name", thorName } }
{ {"component", "eclccserver" }, { "name", instanceName } }
```

### User-specific dimensions (job execution metrics):
```cpp
{ {"component", "thor" }, { "name", thorName }, { "user", username } }
{ {"component", "eclccserver" }, { "name", instanceName }, { "user", username } }
```

### Other dimension examples:
```cpp
{ {"plane", "data1"}, {"user", "gavin"} }  // File access by data plane and user
```

Multiple sets of metrics are recorded for each component corresponding to different grouping conditions, so multiple sets need to be combined to gain the full picture for a given time range. Each set of metrics is the sum for all replicas of the same named component.

## Component lifecycle metrics (user-independent)

These metrics track the operational lifecycle of service components and have dimensions like `{ {"component", "thor" }, { "name", thorName } }`:

- **StNumStarts** - Number of component replicas started in this time period
- **StNumWaits** - Number of times instances waited for a new job (queue polling attempts)
- **StNumAccepts** - Number of jobs successfully dequeued before timeout. By implication: `StNumWaits - StNumAccepts = StNumStops`
- **StNumStops** - Number of instances that stopped due to timeout or shutdown
- **StTimeStart** - Startup time from master starting until workers are provisioned and ready to run jobs
- **StCostStart** - Expense incurred by resources during startup and provisioning phase
- **StTimeProvision** - Time spent provisioning all workers for replicas started in this period (Thor only)
- **StTimeWaitSuccess** - Total time instances waited until successfully dequeuing a job
- **StTimeWaitFailure** - Total time instances waited until timing out and stopping
- **StCostWait** - Cost associated with instances waiting for jobs (both successful and failed waits)

## Job execution metrics (user-specific)

These metrics track actual job processing and include user information in dimensions like `{ {"component", "thor" }, { "name", thorName }, { "user", username } }`:

- **StNumSuccesses** - Number of jobs that completed successfully
- **StNumFailures** - Number of jobs that failed during execution
- **StNumAborts** - Number of jobs that were aborted (by user or system)
- **StTimeLocalExecute** - Elapsed time to process jobs (excludes wait time)
- **StCostExecute** - Total cost of job execution (for successful and failed jobs)
- **StCostAbort** - Cost of jobs that were subsequently aborted

## Component naming conventions

### Thor components:
Format: `"thor<number-of-workers>-<instance-type>-<suffix>"`
- Example: `"thor40-d48-a"` has 40 workers using d48 instance type
- To calculate per-worker metrics, divide the aggregate metrics by the number of workers

### ECL Compiler components:
Format: `"eclccserver-<instance-identifier>"`
- Example: Instance name from configuration

## Usage patterns by component

### Thor (thorlcr/master/thgraphmanager.cpp):
- Records component lifecycle metrics when accepting/rejecting jobs from queue
- Records execution metrics after job completion with success/failure/abort status
- Uses "Queue" category for all metrics

### ECL Compiler Server (ecl/eclccserver/eclccserver.cpp):
- Records startup metrics when service starts
- Records wait/accept metrics for compilation job queue
- Records compilation execution metrics with success/failure/abort status
- Uses "Queue" category for all metrics

### Test scenarios (testing/unittests/dalitests.cpp):
- Demonstrates various dimension combinations
- Shows usage with custom categories like "testCategory", "testCategory2"
- Examples of file access metrics by data plane

## Important implementation notes

- The system automatically handles time bucketing into hourly slots (YYYYMMDDHH format)
- Metrics are aggregated across all replicas of the same named component
- The function calls are atomic and safe for concurrent use
- Failed calls (e.g., Dali connection issues) fail silently to avoid blocking components
- All StatisticKind values used must be defined in `system/jlib/jstatcodes.h`
