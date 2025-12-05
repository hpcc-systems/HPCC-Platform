# Sasha Global Message Management

## Overview

Sasha includes a Global Message Management service that automatically manages the lifecycle of global messages in the HPCC Platform. This periodic background service runs at regular intervals to maintain system-wide notifications, handling hiding, archival, and deletion of old messages based on configurable retention policies.

The service follows the same pattern as other Sasha housekeeping services (like debug plane housekeeping), running as a background thread that performs maintenance operations according to a configurable schedule.

## Features

The Global Message management system provides the following capabilities:

1. **Periodic Maintenance** - Automatic background processing of message lifecycle
2. **Configurable Retention** - Define how long messages are kept active  
3. **Auto-Hide** - Hide old messages while preserving them for audit
4. **Auto-Archival** - Export old messages to CSV files before deletion
5. **Auto-Delete** - Remove very old messages beyond retention period
6. **Flexible Configuration** - Customizable policies per deployment

## Configuration

The Global Message service is configured through the component configuration system, similar to other Sasha services like debug housekeeping. Configuration is loaded using `getComponentConfig()` and follows the standard HPCC configuration patterns.

### Configuration Parameters

- `@interval` (hours, default: 24): How often the maintenance cycle runs
- `@retentionDays` (days, default: 30): How long to keep messages before deletion
- `@hideAfterDays` (days, default: 7): How long before messages are automatically hidden
- `@archiveAfterDays` (days, default: 14): How long before messages are archived
- `@archivePath` (string, default: "/var/lib/HPCCSystems/globalmessages"): Directory for archived messages
- `@enableArchiving` (boolean, default: false): Whether to archive messages before deletion
- `@enableAutoDelete` (boolean, default: false): Whether to automatically delete old messages
- `@enableHideOldMessages` (boolean, default: true): Whether to automatically hide old messages

### Example Configuration (Containerized)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: global-message-config
data:
  # Global Message maintenance configuration
  interval: "24"          # Run daily (hours)
  retentionDays: "90"      # Keep messages for 90 days
  hideAfterDays: "14"      # Hide messages after 2 weeks
  archiveAfterDays: "30"   # Archive messages after 30 days
  archivePath: "/var/lib/HPCCSystems/globalmessages"
  enableArchiving: "true"
  enableAutoDelete: "true"
  enableHideOldMessages: "true"
```

### Example Configuration (Bare Metal)

```xml
<!-- In environment.xml or component configuration -->
<GlobalMessageServer interval="24" 
                     retentionDays="60"
                     hideAfterDays="7"
                     archiveAfterDays="21"
                     archivePath="/var/lib/HPCCSystems/globalmessages"
                     enableArchiving="true"
                     enableAutoDelete="true"
                     enableHideOldMessages="true" />
```

## Service Behavior

The Global Message service is a periodic background service that follows the same pattern as other Sasha housekeeping services (like `CSashaDebugPlaneHousekeepingServer`). Key characteristics:

### Scheduling
- Uses `CSashaSchedule` utility for consistent timing control
- Supports both interval-based and cron-style scheduling
- Runs as a background thread with proper synchronization
- Service can be disabled by setting interval to 0

### Startup and Shutdown
- Service starts automatically when Sasha starts (if enabled)
- Graceful shutdown with proper thread termination
- Configuration is loaded at startup from component config
- Service will not start if interval is set to 0 (disabled)

### Maintenance Cycle
The service performs three types of maintenance operations in sequence:

1. **Hide Old Messages** (if enabled):
   - Uses efficient SysInfoLogger filter API to hide messages in bulk
   - Marks messages as hidden after configured age
   - Hidden messages don't appear in normal queries
   - Default: messages hidden after 7 days

2. **Archive Old Messages** (if enabled):
   - Creates timestamped CSV archive files
   - Exports messages older than `archiveAfterDays` to structured format with proper escaping
   - **Deletes archived messages from Dali storage** after successful export
   - Only runs if archiving is enabled
   - Archive includes message metadata and timestamp header

3. **Delete Old Messages** (if enabled):
   - Permanently removes messages older than retention period
   - Default: messages deleted after 30 days
   - Only runs if auto-delete is enabled

## Archive Files

When archiving is enabled, messages are exported to timestamped CSV files:
```
/var/lib/HPCCSystems/globalmessages/globalmessages_20241201_143052.txt
```

Archive format (CSV with proper escaping):
```
# HPCC Global Messages Archive
# Created: 2024-12-01 14:30:52
# Messages older than: 2024-10-01
# Format: ID,Timestamp,Source,Severity,Code,Hidden,Message

12345,1701432645,"MyApp","warning",100,"false","System maintenance scheduled"
12346,1701432700,"GlobalMessage","error",0,"false","Critical system alert"
```

**Important**: When archiving is enabled, messages are **moved** from Dali storage to archive files. They are no longer available in the system after archiving.

## Containerized Deployment

For containerized deployments, Global Message management is available as a separate service:

```yaml
- name: global-message-housekeeping
  service: global-message-housekeeping
  # Configuration through ConfigMap
  configMap: global-message-config
```

## Implementation Details

### Core Components

1. **CSashaGlobalMessageServer**: Main service class implementing ISashaServer interface
2. **SysInfoLogger Integration**: Uses efficient filter API for bulk operations
3. **CSashaSchedule**: Handles timing and scheduling like other Sasha services
4. **Component Configuration**: Uses standard HPCC configuration patterns

### Service Pattern
The implementation follows the same pattern as `CSashaDebugPlaneHousekeepingServer`:
- Inherits from ISashaServer and Thread
- Uses CSashaSchedule for timing control
- Implements start(), ready(), stop() lifecycle methods
- Runs maintenance cycle in background thread

### Efficient Operations
- **Hide Messages**: Uses `createSysInfoLoggerMsgFilter()` with `setOlderThanDate()` for bulk hiding
- **Archive Messages**: Iterates through filtered messages with proper CSV escaping
- **Delete Messages**: Uses `deleteOlderThanLogSysInfoMsg()` for efficient bulk deletion

## Integration Points

- **SysInfoLogger**: Backend storage, filtering, and bulk operations
- **CSashaSchedule**: Timing and scheduling utility
- **Component Configuration**: Standard HPCC configuration loading
- **Dali**: Persistent storage through SDS

## Monitoring and Troubleshooting

### Log Messages
The Global Message service logs its activities:
```
Global Message Server Configuration:
  Interval: 24 hours
  Retention Period: 30 days (messages older than this are deleted)
  Archive Path: /var/log/hpcc/globalmessages
  Archiving: enabled
  Auto Delete: enabled
  Auto Hide: enabled
  Hide After: 7 days
Starting Global Message maintenance cycle
Global Message maintenance: hidden 15, archived 5, deleted 2 messages
```

### Performance Considerations
- Archive directory should have sufficient storage for long-term message retention
- Consider archive file rotation for long-running systems
- Monitor maintenance cycle duration for very large message volumes
- Archive path should be on fast, reliable storage
- **Archive files are the only copy after archiving** - ensure proper backup of archive directory
- Plan archive storage capacity based on message volume and retention requirements

### Troubleshooting
- Check Sasha logs for maintenance cycle messages
- Verify archive directory permissions and disk space
- Validate configuration syntax and parameter ranges
- Service can be disabled by setting interval to 0

## Message Lifecycle

1. **Active Phase**: New messages are visible to all users
2. **Hidden Phase**: After `hideAfterDays`, messages become hidden but remain in Dali storage
3. **Archive Phase**: After `archiveAfterDays`, messages are exported to CSV files and **removed from Dali storage**
4. **Deletion Phase**: After `retentionDays`, any remaining messages are permanently removed

**Important**: Archiving moves messages from Dali to external files - archived messages are no longer stored in Dali.

## File Structure

### Implementation Files
- `saglobalmsg.hpp`: Service interface declaration
- `saglobalmsg.cpp`: Service implementation following housekeeping pattern
- `saserver.cpp`: Service instantiation (createSashaGlobalMessageServer)

### Service Registration
The service is registered in the containerized service list:
```cpp
else if (strieq(service, "global-message-housekeeping"))
   servers.append(*createSashaGlobalMessageServer());
```

## Future Enhancements

Potential improvements include:

1. **Secondary Dali Storage**: Custom implementation for archiving to secondary Dali instance
2. **Compression**: Compress archive files to save storage space
3. **Cleanup Policies**: Automatic cleanup of old archive files
4. **Metrics**: Expose maintenance operation metrics
5. **Configuration Validation**: Enhanced validation of configuration parameters
6. **Recovery**: Ability to restore messages from archive files

## Security Considerations

- Archive files contain message content - ensure appropriate file permissions
- Archive directory should be secured and backed up regularly
- Consider archive file encryption for sensitive environments
- All maintenance operations are logged for audit purposes

## Background Service vs Command API

**Important**: The Global Message service runs as a background service only. Unlike some other Sasha services, it does not process commands through the Sasha command interface. Message management is handled automatically according to the configured schedule and retention policies.

This design ensures:
- Consistent maintenance without manual intervention
- Reduced operational complexity
- Alignment with other Sasha housekeeping services
- Reliable operation in containerized environments