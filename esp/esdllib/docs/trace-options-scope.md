#### trace-options-scope
```xml
    <trace-options-scope
        enabled="XPath expression evaluable to Boolean"
        locked="XPath expression evaluable to Boolean"
    >
        <!-- affected child operations -->
    </trace-options-scope>
```

An operation with children, creates a temporary scope within which changes to trace availability may be applied when such changes are not prohibited by the containing scope. All changes made within the operation or its children are discarded when operation processing completes.

Absent any configuration to the contrary, the *trace* operations are available. A process, or service, that processes scripts has the opportunity to change the default availability, as well as make its choice immutable.

> The default availability might be changed for cloud deployments, where cost of output is a concern, or in production environments, when timing is critical. The default might be made immutable if the output will not be available for review, or if data masking is either not configured or the configured masking is incompatible with the data (e.g., UK data baing masked by a US masking profile).

This operation has no effect if either the default configuration or a previous instance of the operation requests immutability.

| Property | Count | Description |
| :- | :-: | :- |
| `@enabled` | 0..1 | XPath expression determining the availability (*true*) or unavailability (*false*) of subsequent *trace* operations. An omitted or empty value retains the current state. |
| `@locked` | 0..1 | XPath expression controlling whether the availability can be changed by subsequent requests. An omitted or empty value retains the current state. |
