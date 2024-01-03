# HPCC Component Instrumentation and Tracing

The HPCC Platform is instrumented to optionally track distributed work actions (Traces) and their various sub-task (spans) via Open Telemetry tooling. Traces and spans track information vital information which can be reported to services which specialize in processing and visualization of standardized trace/span information for the purpose or monitoring and observability of the health of individual requests processed by the platform.

Tracing of HPCC components is enabled by default, and can be configured to fit the needs of the given deployment.

## Configuration
All configuration options detailed here are part of the HPCC Systems Helm chart, and apply at the global or component level.

### Tracing cofiguration options
- disabled - (default: false) disables tracking and reporting of internal traces and spans
- alwaysCreateGlobalIds - If true, assign newly created global ID to any requests that do not supply one.
- optAlwaysCreateTraceIds - If true components generate trace/span ids if none are provided by the remote caller.
- enableDefaultLogExporter - If true, creates a trace exporter outputting to the log using the default options
- exporters: - Defines a list of exporters in charge of forwarding span data to target back-end
  - type - "OTLP-HTTP" | "OTLP-GRPC" | "OS" | "JLOG"
    - "JLOG"
      - logSpanDetails - Log span details such as description, status, kind
      - logParentInfo  - Log the span's parent info such as ParentSpanId, and TraceState
      - logAttributes  - Log the span's attributes
      - logEvents      - Log the span's events
      - logLinks       - Log the span's links
      - logResources   - Log the span's resources such as telemetry.sdk version, name, language
    - "OTLP-HTTP"
      - endpoint - (default localhost:4318) Specifies the target OTLP-HTTP backend
      - timeOutSecs - (default 10secs)
      - consoleDebug - (default false)
    - "OTLP-GRPC"
      - endpoint: (default localhost:4317) The endpoint to export to. By default the OpenTelemetry Collector's default endpoint.
      - useSslCredentials - By default when false, uses grpc::InsecureChannelCredentials; If true uses sslCredentialsCACertPath
      - sslCredentialsCACertPath - Path to .pem file to be used for SSL encryption.
      - timeOutSeconds - (default 10secs) Timeout for grpc deadline
  - batch:
    - enabled - If true, trace data is processed in a batch, if false, trace data is processed immediately

### Sample configuration
Below is a sample helm values block directing the HPCC tracing framework to process span information serially, and export the data over OTLP/HTTP protocol to localhost:4318 and output export debug information to console:

```console
global:
  tracing:
    exporters:
    - type: OTLP-HTTP
      consoleDebug: true
```
### Sample configuration command

Sample helm command deploying an HPCC chart named myTracedHPCC using the hpcc helm repo and providing a the above tracing configuration.

```console
helm install myTracedHPCC hpcc/hpcc -f otlp-http-collector-default.yaml
```
## Tracing information
HPCC tracing information includes data needed to trace requests as they traverse over distributed components, and detailed information pertaining to important request subtasks in the form of span information. Each trace and all its related spans are assigned unique IDs which follow the Open Telemetry standard.

Tracing information can be exported to various Open Telemetry compatible endpoints including HPCC component logs, or OTLP collectors, etc. By default, tracing information is configured to be exported to HPCC component logs.

Sample span reported as log event:
```console
00000165 MON EVT 2023-12-01 17:19:07.270     8   688 UNK     "{ "name": "HTTPRequest", "trace_id": "891070fc4a9ef5a3751c19c555d7d4a8", "span_id": "23a47b5bb486ce58", "start": 1701451147269962337, "duration": 652093, "Attributes": {"http.request.method": "GET","hpcc.localid": "JJmSnTeFWTQL8ft9DcbYDK","hpcc.globalid": "JJmSnTedcRZ99RtnwWGwPN" } }""
```

Each log statement includes a timestamp denoting the span start time, and a duration along with  the span name, trace and span id, and any HPCC specific attribute such as legacy GlobalID (if any), HPCC CallerID (if any), LocalID (if any).
The span info logged can be expanded to include span resources, events, and other details (see configuration details).

Spans exported via exporters will contain more detailed information such as explicit start time, duration, and any other attribute assigned to the span by the component instrumentation.

Sample exported span data:
```json
{
  "name": "HTTPRequest",
  "trace_id": "53f47047517e9dd9f5ad8c318b4b4fe0",
  "span_id": "b9489283b66c1073",
  "start": 1701456073994968800,
  "duration": 1002426,
  "attributes": {
    "http.request.method": "GET",
    "hpcc.localid": "JJmvRRBJ1QYU8o4xe1sgxJ",
    "hpcc.globalid": "JJmvRRBjnJGY6vgkjkAjJc"
  },
  "events": [
    {
      "name": "Acquiring lock",
      "time_stamp": 1701456073995175400,
      "attributes": {
        "lock_name": "resourcelock"
      }
    },
    {
      "name": "Got lock, doing work...",
      "time_stamp": 1701456073995269400,
      "attributes": {
        "lock_name": "resourcelock"
      }
    },
    {
      "name": "Release lock",
      "time_stamp": 1701456073996269400,
      "attributes": {
        "lock_name": "resourcelock"
      }
    }
  ],
  "resources": {
    "service.name": "unknown_service",
    "telemetry.sdk.version": "1.9.1",
    "telemetry.sdk.name": "opentelemetry",
    "telemetry.sdk.language": "cpp"
  }
}
```

## Directory Contents

- 'otlp-http-collector-default.yaml' - Sample tracing configuration targeting OTLP/HTTP trace collector
- 'otlp-grpc-collector-default.yaml' - Sample tracing configuration targeting OTLP/GRPC trace collector
- 'jlog-collector-fulloutput.yaml' - Sample tracing configuration targeting HPCC component logs
