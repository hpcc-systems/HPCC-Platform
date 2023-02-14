# Available global/expert settings in HPCC helm chart

The 'expert' section under 'global' of the values.yaml should be used to define low-level, testing or developer settings,
i.e. in most deployments, it should remain empty.

This is an example of what the global/expert section might look like:
```
global:
  expert:
    numRenameRetries: 3
    maxConnections: 10
    keepalive:
      time: 200
      interval: 75
      probes: 9
```

NB: Some components (e.g. DfuServer and Thor) also have an 'expert' settings area (see values schema) that can be used for relavent settings
on a per component instance basis, rather than setting them globally.


The following options are currently available:

## numRenameRetries (unsigned)

If set to a positive number, the platform will re-attempt to perform a rename of a physical file on failure (after a short delay).
This should not normally be needed, but on some file systems it may help mitigate issues where the file has just been closed and not exposed
correctly at the posix layer.

## maxConnections (unsigned)

This is a DFU Server setting.
If set, it will limit the maximum number of parallel connections and partition streams that will be active at any one time.
By default a DFU job will run as many active connection/streams as there are partitions involved in the spray, limited to an absolute maximum of 800.
Setting maxConnections can be set to reduce this concurrency.
This might be helpful in some scenarios where the concurrency is causing network congestion and degraded performance.

## keepalive (time: unsigned, interval: unsigned, probes: unsigned)

See keepalive example above.
If set, these settings will override the system default socket keepalive settings each time the platform creates a socket.
This may be useful in some scenarios if the connections would otherwise be closed prematurely by external factors (e.g. firewalls).
An example of this is that Azure instances will close sockets that have been idle for greater than 4 minutes that are connected
outside of its networks.

