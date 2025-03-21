# Sasha

## Services

### Debug Plane Housekeeping Server

#### Overview

- This is a maintenance service that automatically manages files and directories on the debug plane using a configured retention period.
- This is required to control the disc space used by post-mortem and other files on the debug plane.
- This service is designed to execute on a schedule with no interaction other; a single log entry for a summary of any deleted files or directories will be generated.

#### Installation and Configuration

- The service is installed and active by default.
- It is configuration-driven with fallback to defaults in the configuration yaml files.
  - For example `values.yaml` :

    |attribute|description|values|default|required|
    |:--------|:----------|:-----|:------|:-------|
    |disabled|Disable or enable this service.|false|false|optional|
    |interval|Minimum interval between debug plane housekeeping expiry checking (in hours, 0 disables).|24||optional|
    |at|Schedule to check for debug plane expired files and directories (cron format).|* 3 * * *||optional|
    |expiryDays|Default number of days passed that debug plane files or directories are deleted.|14|14|optional|

#### User Guide

##### How it works in steps

1. The `disabled` value is read from the component configuration
2. If `disabled` is `true` then the service is not started.
3. The schedule is read from the component configuration.
4. If the `interval` is 0 then the service is stopped.
5. The debug plane location is read from the component configuration.
6. The service `Thread` is started.
7. A loop of the following is started until the service is stopped:
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