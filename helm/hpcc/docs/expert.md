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
    regex:
      cacheSize: 500
    memoryCoreDump:
      mode: auto # { auto, on, off }
      intervalSecs: 10
      #thresholdMB: 7500 # supersedes 'auto' mode if set
      #incrementMB: 100
      #useVforkAndGcore: true # (default: false) uses vfork()+exec(gcore), instead of fork()+abort()
      #suspendParent: true # (default: false). Suspend parent process during core dump (involves intermediate reaper process)
```

NB: Some components (e.g. DfuServer and Thor) also have an 'expert' settings area (see values schema) that can be used for relavent settings
on a per component instance basis, rather than setting them globally.

Planes can also have an expert section (see Plane Expert Settings section)


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

## saveQueryDlls (boolean)

This is a Thor only setting. Default: false
If false, query dlls are loaded directly from the default 'dll' plane by both the Thor manager and Thor workers.
If true, query dlls will be saved and cached in local temporary storage and serialized to the workers.
Saving and serializing the query dlls may speed up queries if the 'dll' plane is backed by slow storage (e.g. blob storage).

## exceptionHandler (list of { class: string, code: unsigned, cmd: string })

Exception handlers can be added at a global or per component level.
Each exception handler must define the 'class' (one of "string", "errno", "os", "socket") and 'code' number of the exception to handle, and the command to run ('cmd').
Example exception handler configured for a disk full exception :
```
exceptionHandler:
- class: "os"
  code: 28
  cmd: "bash -c 'ls -lt /var/lib/HPCCSystems; echo next; ls -lt /var/lib/HPCCSystems/hpcc-spill'"
```

Handled exceptions will run the defined command and capture the output in a file in the debug plane with a filename of the following form: "exception-\<code\>-\<datestamp\>.log"

## allowForeign (boolean)

Foreign file reads (~foreign::) are forbidden by default since the official santioned secure route is to use the DFS
service via remote file reads with the ~remote:: syntax.
Setting expert.allowForeign to true, enables foreign access for compatibility with legacy bare-metal environments
that have their Dali and Dafilesrv's open.

## regex (cacheSize: unsigned)

See the regex example above.  If set, this should be added at the global level.
The default value is 500.  Set to zero to disable the cache.
This value is applied at the process level:  Each Thor worker, Roxie process, and hthor worker receives
its own cache.  Threads/channels within a process share that process's cache.


# Plane Expert Settings

## validatePlaneScript (list of { string })

Optional list of bash commands to execute within an init container in pods that use this plane.
This can be used to validate that the plane is healthy, e.g. that it is mounted as expected.
If the script returns a non-zero result, the init container and therefore the pod will fail.

## blockedFileIOKB (unsigned)

The optimal size to read and write sequential file io (e.g. for Azure Blob storage set to 4096)

## blockedRandomIOKB (unsigned)

The optimal size of random file io reads (e.g. index lookups).

## fileSyncWriteClose (boolean)

Perform a fsync ahead of file close operations.
Default: false

## concurrentWriteSupport (boolean)

Plane supports concurrent writing to a single physical file.
Default: false

## writeSyncMarginMs (unsigned)

Minimum time period between the publication of a logical file and when it can
be read. This setting will introduce a delay if a read operation is within this
margin period.
Should be set on planes backed by storage types that do not guarantee files are
ready to be read by any other consumer immediately, e.g. Azure Blob storage.
Default: 0

## safeStatBehaviour (unsigned)

Controls error behaviour of jfile stat() calls.
0 = fail on any unexpected error. NB: ENOENT and ENOTDIR as file not present.
1 = ignore EACCES - suppre exception, returns false.
2 = ignore all other errors - suppress exception, return false. This was the legacy behaviour.
Default: 0

## renameSupported (boolean)

Plane supports physical file part renaming.
Default: based on plane configuration. Planes based with 'pvc' and/or storageapi default to false. All others to true.

## memoryCoreDump (mode: string, intervalSecs: unsigned, thresholdMB: unsigned, incrementMB: unsigned, useVforkAndGcore: boolean, suspendParent: boolean)

See memoryCoreDump example above.
Defaulted off.
If enabled, monitors total memory usage once per 'intervalSecs' and if above 'thresholdMB' creates a core dump.
Further core files will be created if 'incrementMB' is set, and memory increases by more than that amount.
mode=auto will auto-config 'thresholdMB' to 95% of total memory, and 'incrementMB' to 4% of total memory,
meaning there will be at most 2 cores produced, one if detected >95% and another if detected >99%.
