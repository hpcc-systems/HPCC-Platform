# Roxie Trace Levels Overview

This document summarizes how `traceLevel` is used across the `roxie` directory of the HPCC-Platform repository, as well as miscellaneous trace settings related to specific subsystems.

---

## Table 1: Instances of `traceLevel` and Their Effects

| Condition/Comparison  | Where Used / Component                            | Purpose / Effect                                                 | Example Source/Link                                                                                  |
|----------------------|---------------------------------------------------|------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `if (traceLevel)`    | Roxie main code, listeners, topology managers     | General tracing/logging enabled for most Roxie server events     | [ccdlistener.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/ccd/ccdlistener.cpp) |
| `traceLevel > 0`     | Job queue management, worker threads, etc.        | Enables tracing/log messages for workunit or Dali queue actions  | [ccdlistener.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/ccd/ccdlistener.cpp) |
| `traceLevel > 1`     | CCDCACHE utility component (not core Roxie)       | More detailed cache warming operations tracing                   | [ccdcache.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/ccdcache/ccdcache.cpp)  |
| `traceLevel >= 5`    | Roxie topology server (toposerver)                | Extra verbosity: receiving requests, sending digests/responses   | [toposerver.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/topo/toposerver.cpp)  |
| `traceLevel >= 6`    | Roxie topology server (toposerver)                | Very detailed (per-entry) topology trace logs                    | [toposerver.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/topo/toposerver.cpp)  |
| Default set (eg. `1`)| ccdmain.cpp/YAML/configs                          | Enables standard trace logging                                   | [ccdmain.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/ccd/ccdmain.cpp)         |

---

## Table 2: Miscellaneous Trace Settings in Roxie

| Variable Name         | Default Value | Set/Used Where                    | Purpose / Effect                                             |
|----------------------|---------------|-----------------------------------|--------------------------------------------------------------|
| `udpTraceLevel`      | `1`           | ccdmain.cpp (`@udpTraceLevel`)    | UDP networking trace level for Roxie agent/server comms       |
| `memTraceLevel`      | `1`           | ccdmain.cpp (`@memTraceLevel`)    | Memory-related tracing, mainly in roxiemem module             |
| `soapTraceLevel`     | `1`           | ccdmain.cpp (`@soapTraceLevel`)   | SOAP protocol (web service) tracing/logging                   |
| `miscDebugTraceLevel`| `0`           | ccdmain.cpp (`@miscDebugTraceLevel`)| Extra, miscellaneous debug output (purpose can vary)        |
| `udpTraceFlow`       | `false`       | ccdmain.cpp (`@udpTraceFlow`)     | Boolean: traces per-message UDP network flow                  |
| `udpTraceTimeouts`   | `false`       | ccdmain.cpp (`@udpTraceTimeouts`) | Boolean: traces timeouts on UDP communication                 |
| `traceRemoteFiles`   | `false`       | ccdmain.cpp (`@traceRemoteFiles`) | Boolean: enables tracing for remote file operations           |

---

**Notes:**
- For `traceLevel`, the actual effect depends on thresholds tested in various code locations. Nonzero generally enables tracing, higher numbers (5, 6) unlock additional verbosity in specific modules (like toposerver).
- Miscellaneous settings target specific subsystems and may be boolean or integer levels.
- For a comprehensive, current list and default values, check your Roxie configuration and the latest code in [roxie/ccd/ccdmain.cpp](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/ccd/ccdmain.cpp).
