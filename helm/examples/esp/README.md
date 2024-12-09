# ESP Trace Flags

Each ESP process may specify its own set of flags for controlling trace behavior. The specific flags may be shared with other platform components, shared amongst ESPs, or unique to an ESP.

## Accepted Flags

Detailed description of flags used by any ESP.

### Shared Platform Flags

Flags defined in `system/jlib/jtrace.hpp` and used by multiple platform processes.

Flags will be added to this list as tracing logic is updated in ESP code. For example, the shared platform flag `traceHttp` is expected to be used, as are a number of ESP-specific options.

### Shared ESP Flags

Flags defined in `esp/platform/esptrace.h` and applicable to most, if not all, ESP configurations.

#### traceDetail

Set the default trace level in the process. Accepted case-insensitive values are:
- `1`, `standard`: most important output
- `2`, `detailed`: average verbosity output
- `3`, `max`: highest verbosity output
- `default`: use existing level
  - Release builds default to `standard`
  - Debug builds default to `max`
- `0`, `none`, *all other values*: no trace output

## Process Configuration

### Containerized

#### esp.yaml

Each ESP application's configuration object may embed one instance of a `traceFlags` object. Within this object, at most one property per [accepted flag](#accepted-flags) is expected. Properties not described here are ignored.

For example, the `eclwatch` process might be configured to use detailed reporting like this:

```yml
esp:
- name: eclwatch
  traceFlags:
    traceDetail: 2
```

## Cluster Overrides

A values file may be used with the `helm install` command to override the settings of all ESPs. The `--set` option may be used to target the settings of a specific ESP in the configured array.

### Bare-Metal

No support for defining trace flags is provided by the `configmgr` application. Within a stand-alone `esp.xml` file, however, a `traceFlags` child of the `EspProcess` element may be defined.

The previous YAML example may be reproduced in XML with the following:

```xml
<EspProcess ...>
  <traceFlags traceDetail="2" />
  ...
<EspProcess>
```
