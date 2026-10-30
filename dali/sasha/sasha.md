# Sasha

## Services

### Archiver Services

#### Overview

- The Sasha archiver is responsible for lifecycle management of archived metadata and files for WorkUnits and DFU data.
- It supports both scheduled background archiving and command-driven archive/backup/restore/list flows.
- In bare-metal mode, one combined server runs all archiver roles; in containerized mode, roles are split into dedicated servers.

#### Main Entry Points

- Server factory declarations: `dali/sasha/saarch.hpp`
    - `createSashaArchiverServer()` (bare-metal combined server)
    - `createSashaWUArchiverServer()` (containerized WU archiver)
    - `createSashaDFUWUArchiverServer()` (containerized DFU WU archiver)
    - `createSashaDFURecoveryArchiverServer()` (containerized DFU recovery archiver)
    - `createSashaCachedWURemoverServer()` (containerized cached WU cleanup)
- Command dispatcher: `processArchiverCommand(ISashaCommand *cmd)` in `dali/sasha/saarch.cpp`
- WU listing/query helper used by command actions: `WUiterate(ISashaCommand *cmd, const char *mask)`

#### Runtime Topology

1. Bare-metal topology:
     - `CSashaCombinedArchiverServer` creates and runs:
         - `CWorkUnitArchiver`
         - `CDFUWorkUnitArchiver`
         - `CDFUrecoveryArchiver`
         - `CCachedWorkUnitRemover`
     - The server loops every minute, checks each archiver schedule, then runs eligible archivers.

2. Containerized topology:
     - `CSashaArchiverServer` is a base single-role threaded server.
     - Specialized servers each own one archiver instance and one SDS subtree (`xpath`):
         - `CSashaWUArchiverServer` -> `WorkUnits/`
         - `CSashaDFUWUArchiverServer` -> `DFU/WorkUnits/`
         - `CSashaDFURecoveryArchiverServer` -> `DFU/RECOVERY/job`
         - `CSashaCachedWURemoverServer` -> `CachedWorkUnits/`

#### Core Archiver Framework

- Abstract scheduling/scanning logic is centralized in `CBranchArchiver`.
- Each concrete branch archiver supplies branch-specific behavior via `createBranchItem()` and item implementation.
- `CBranchArchiver::action()` performs:
    1. SDS branch scan (`branchxpath`)
    2. Per-item qualification (`qualifies()`) for archive and optional backup (`qualifiesBackup()`)
    3. Sort/prioritization using `compare()`
    4. Rate/duration control via `CSashaSchedule`
    5. Archive and backup execution loops

#### Branch Item Contract

- `IBranchItem` defines the per-record decision and execution interface:
    - `isempty()`
    - `qualifies()`
    - `qualifiesBackup()`
    - `archive()`
    - `backup()`
    - `compare(IBranchItem &to)`
- Concrete item implementations:
    - `CWorkUnitArchiver::cWUBranchItem`
    - `CDFUWorkUnitArchiver::cDFUWUBranchItem`
    - `CDFUrecoveryArchiver::cDRBranchItem`

#### WorkUnit Archiving Flow

1. Scheduler path:
     - `CWorkUnitArchiver` scans `WorkUnits/*`.
     - Each WU is wrapped by `cWUBranchItem`.
     - `qualifies()` gates by protection/time/cutoff and state eligibility.

2. Archive execution:
     - `CWorkUnitArchiver::archive()` calls `doArchiveWorkUnit()`.
     - `doArchiveWorkUnit()`:
         - Acquires the archive mutex before opening the WU, matching the restore lock order
         - Opens WU via `IWorkUnitFactory`
         - Optionally version-renames existing LDS files
         - Builds an archive metadata snapshot (when publishing is enabled)
         - Calls `IExtendedWUInterface::archiveWorkUnit()`
         - Publishes the snapshot only after `archiveWorkUnit()` succeeds; a publish failure is logged and does not undo the archive

3. Archive publishing (optional):
     - Publisher interface: `dali/sasha/wuarchivepublisher.hpp`

#### DFU and Recovery Flows

- DFU WorkUnits:
    - Archiver: `CDFUWorkUnitArchiver`
    - Archive operation: `doArchiveDfuWorkUnit()` writes XML to `Archive/DFUWorkUnits/...`
- DFU Recovery:
    - Archiver: `CDFUrecoveryArchiver`
    - Archive operation writes XML to `Archive/DFUrecovery/...` and removes original node (when enabled)
- Cached WU cleanup:
    - `CCachedWorkUnitRemover` overrides `action()` to prune older cached entries beyond configured limits.

#### Command-Driven Flow

- `processArchiverCommand()` handles interactive/remote command actions:
    - `SCA_ARCHIVE`, `SCA_BACKUP`, `SCA_RESTORE`
        - WU: `doArchiveWorkUnit()` / `doRestoreWorkUnit()`
        - DFU: `doArchiveDfuWorkUnit()` / `doRestoreDfuWorkUnit()`
    - `SCA_LIST`, `SCA_GET`, `SCA_WORKUNIT_SERVICES_GET`, `SCA_LISTDT`
        - Delegates to `WUiterate()`
- Results are appended to command IDs for compatibility with existing consumers.

#### Synchronization and Error Handling

- The shared `CriticalSection` (`archivingSect`) serializes DFU WorkUnit archive/restore operations within the process and is used for WorkUnit archive/restore synchronization.
- Each server thread catches `IException`, logs via `EXCLOG`, and requests server stop on unrecoverable errors.
- WorkUnit archive failures mark SDS metadata (`@archiveError`, `@archiveErrorDate`) for retry logic.

#### Configuration Concepts

- Scheduling/limits are read per archiver branch from config (`interval`, `at`, `limit`, `cutoff`, optional `backup`).
- WU archiver-specific options include `keepResultFiles` and `retryinterval`.
- Archive publishing is provided by a separate publisher abstraction.

#### Practical File Map

- Archiver interfaces and factories: `dali/sasha/saarch.hpp`
- Main implementation and orchestration: `dali/sasha/saarch.cpp`
- Command model: `dali/sasha/sacmd.hpp`, `dali/sasha/sacmd.cpp`
- Archive publisher interface: `dali/sasha/wuarchivepublisher.hpp`
- Archive publisher factory: `dali/sasha/wuarchivepublisher.cpp`

### Debug Plane Housekeeping Server

#### Overview

- This is a maintenance service that automatically manages files and directories on the debug plane using a configured retention period.
- This is required to control the disc space used by post-mortem and other files on the debug plane.
- This service is designed to execute on a schedule; a single log entry for a summary of any deleted (or failed to be deleted) files or deleted directories will be generated.

#### Installation and Configuration

- In containerized mode, the Helm chart deploys the service by default; runtime defaults are shown below.
  - For example `values.yaml` :

    |attribute|description|default|required|
    |:--------|:----------|:------|:-------|
    |disabled|When `true`, Helm omits this service's deployment.|false|optional|
    |interval|Minimum interval between debug plane housekeeping expiry checking (in hours, 0 disables).|24|optional|
    |at|Optional cron schedule for checking expired files and directories.|Unset; interval only|optional|
    |expiryDays|Default number of days passed that debug plane files or directories are deleted.|14|optional|

#### User Guide

##### How it works in steps

1. Helm omits the service deployment if `disabled` is `true`.
2. The service reads `expiryDays` and `interval` from its component configuration.
3. If either value is 0, the service does not start.
4. The debug plane location is read from the component configuration.
5. The service `Thread` is started.
6. A loop of the following is started until the service is stopped:
    1. A one minute wait is imposed.
    2. If the schedule using `at` and `interval` indicates that the housekeeping is required then:
        1. Any exceptions from the following are caught and logged.
            1. The debug plane directory list is read.
            2. The list of files and directories in the debug plane are processed:
                1. If the file or directory's modified date is over `expiryDays` old then:
                    1. The file or directory is deleted.
                    2. A counter for each file or directory is maintained.
            3. A log entry is made giving the total number of files and directories deleted.

##### Inputs and Outputs

- The component configuration as per [Installation and Configuration](#installation-and-configuration) is the input for this service.
- The outputs are any logs generated for errors and deletion statistics.

##### Error Handling

- Known exceptions are handled to prevent service interruption.

#### Limitations and Considerations

- The service ensures it only runs in containerized environments.

#### Future Possible Enhancements

- A bare metal implementation.
- Logging freed up disc space statistics.
