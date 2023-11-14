# HPCC Component Instrumentation and Tracing

The HPCC Platform is instrumented to optionally track distributed work actions (Traces) and their various sub-task (spans) via Open Telemetry tooling. Traces and spans track information vital information which can be reported to services which specialize in processing and visualization of standardized trace/span information for the purpose or monitoring and observability of the health of individual requests processed by the platform.

Tracing of HPCC components is enabled by default, and can be configured to fit the needs of the given deployment.

## Configuration
All configuration options detailed here are part of the HPCC Systems Helm chart, and apply at the global or component level.

### Tracing cofiguration options
- disabled - (default: false) disables tracking and reporting of internal traces and spans
- alwaysCreateGlobalIds - If true, assign newly created global ID to any requests that do not supply one.
- optAlwaysCreateTraceIds - If true components generate trace/span ids if none are provided by the remote caller.
- logSpanStart - If true, generate a log entry whenever a span is started (default: false)
- logSpanFinish - If true, generate a log entry whenever a span is finished (default: true)
- exporter - Defines The type of exporter in charge of forwarding span data to target back-end
 - type - (defalt: NONE) "OTLP-HTTP" | "OTLP-GRCP" | "OS" | "NONE"
  - OTLP-HTTP
   - endpoint - (default localhost:4318) Specifies the target OTLP-HTTP backend
   - timeOutSecs - (default 10secs)
   - consoleDebug - (default false)
  - OTLP-GRCP
   - endpoint: (default localhost:4317) The endpoint to export to. By default the OpenTelemetry Collector's default endpoint.
   - useSslCredentials - By default when false, uses grpc::InsecureChannelCredentials; If true uses sslCredentialsCACertPath
   - sslCredentialsCACertPath - Path to .pem file to be used for SSL encryption.
   - timeOutSeconds - (default 10secs) Timeout for grpc deadline
- processor - Controls span processing style. One by one as available, or in batches.
 - type - (default: simple) "simple" | "batch"

### Sample configuration
Below is a sample helm values block directing the HPCC tracing framework to process span information serially, and export the data over OTLP/HTTP protocol to localhost:4318 and output export debug information to console:

```console
global:
  tracing:
    exporter:
      type: OTLP-HTTP
      consoleDebug: true
    processor:
      type: simple
```
### Sample configuration command

Sample helm command deploying an HPCC chart named myTracedHPCC using the hpcc helm repo and providing a the above tracing configuration.

```console
helm install myTracedHPCC hpcc/hpcc -f otlp-http-collector-default.yaml
```
## Tracing information
HPCC tracing information includes data needed to trace requests as they traverse over distributed components, and detailed information pertaining to important request subtasks in the form of span information. Each trace and all its related spans are assigned unique IDs which follow the Open Telemetry standard.

The start and end of spans are reported to HPCC component logs regardless of any exporter related configuration.

Sample span reported as log event:
```console
000000A3 MON EVT 2023-10-10 22:12:23.827 24212 25115 Span start: {"Type":"Server","Name":"propagatedServerSpan","GlobalID":"IncomingUGID","CallerID":"IncomingCID","LocalID":"JDbF4xnv7LSWDV4Eug1SpJ","TraceID":"beca49ca8f3138a2842e5cf21402bfff","SpanID":"4b960b3e4647da3f"}

000000FF MON EVT 2023-10-10 22:12:24.927 24212 25115 Span end: {"Type":"Server","Name":"propagatedServerSpan","GlobalID":"IncomingUGID","CallerID":"IncomingCID","LocalID":"JDbF4xnv7LSWDV4Eug1SpJ","TraceID":"beca49ca8f3138a2842e5cf21402bfff","SpanID":"4b960b3e4647da3f"}
```

Each log statement denotes the time of the tracing event (start or stop), the span type, name, trace and span id, and any HPCC specific attribute such as legacy GlobalID (if any), HPCC CallerID (if any), LocalID (if any).

Spans exported via exporters will contain more detailed information such as explicit start time, duration, and any other attribute assigned to the span by the component instrumentation.

Sample exported span data:
```json
{
    "Name":"propagatedServerSpan",
    "TraceId":"beca49ca8f3138a2842e5cf21402bfff",
    "SpanId":"6225221529c24252",
    "kind":"Server",
    "ParentSpanId":"4b960b3e4647da3f",
    "Start":1696983526105561763,
    "Duration":1056403,
    "Description":"",
    "Status":"Unset",
    "TraceState":"hpcc=4b960b3e4647da3f",
    "Attributes":{
        "hpcc.callerid":"IncomingCID",
        "hpcc.globalid":"IncomingUGID"
    },
    "Events":{
    },
    "Links":{
    },
    "Resources":{
        "service.name":"unknown_service",
        "telemetry.sdk.version":"1.9.1",
        "telemetry.sdk.name":"opentelemetry",
        "telemetry.sdk.language":"cpp"
    },
    "InstrumentedLibrary":"esp"

}
```

## Directory Contents

- 'otlp-http-collector-default.yaml' - Sample tracing configuration targeting OTLP/HTTP trace collector
