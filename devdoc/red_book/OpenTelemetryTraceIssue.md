# OpenTelemetry (OTel) Trace Issue

A recent change inadvertently removed the "OTel GRPC Trace exporter" which is needed for successful exporting of OTel tracing in certain environments. ([Pull Request](https://github.com/hpcc-systems/HPCC-Platform/pull/20735))

Without the exporter, traces are not exported to their target, so the component logs will not contain meaningful trace transaction information.

A single initialization log line is reported, but can easily be missed.

On bare-metal, jtrace initialization errors are routed to a file. By default, they're captured in this file:

``` text
/var/lib/HPCCSystems/<component-name>/<mm>_<dd>_<yyyy>_<hr>_<min>_<sec>.stderr
```

For example:

```text
/var/lib/HPCCSystems/roxie_09/01_13_2026_10_31_55.stderr
```

``` bash
00000000 OPR 2025-12-11 20:10:08.318 509299 509299 "Exporter type: OTLP-GRPC"
00000001 OPR 2025-12-11 20:10:08.319 509299 509299 "WARNING: Tracing exporter type not supported: 'OTLP-GRPC’"
```

To avoid this issue, make sure you are running:

* 10.0.20 or later
* 9.12.66 or later
* 9.14.46 or later

The above versions have reintegrated the missing GRPC Trace exporter and have resolved the issue.

Full list of versions affected (avoid if planning to export tracing via GRPC):

``` text
 candidate-10.0.0 .. candidate-10.0.18
 candidate-9.12.0 .. candidate-9.12.64
 candidate-9.14.0 .. candidate-9.14.44
```
