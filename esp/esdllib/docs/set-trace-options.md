#### set-trace-options
```xml
    <set-trace-options
        enabled="XPath expression evaluable to Boolean"
        locked="XPath expression evaluable to Boolean"
    />
```

A childless operation that can affect the usability of the `trace`, `trace-content`, and `trace-value` operations. The operations are always accepted in scripts, but their ability to produce output can be controlled by this operation.

Absent any configuration to the contrary, the *trace* operations are available. A process, or service, that processes scripts has the opportunity to change the default availability, as well as make its choice immutable.

> The default availability might be changed for cloud deployments, where cost of output is a concern, or in production environments, when timing is critical. The default might be made immutable if the output will not be available for review, or if data masking is either not configured or the configured masking is incompatible with the data (e.g., UK data baing masked by a US masking profile).

A script may compare runtime data masking capabilities with requirements and either enable or disable subsequent trace operations. This operation allows the availability to be changed within specific constraints:

- If not processed within the scope of a `trace-options-scope` operation, the effects persist for the duration the script context.
- The effects are limited to the scope of an enclosing `trace-options-scope` operation.

This operation has no effect if either the default configuration or a previous instance of the operation requests immutability.

| Property | Count | Description |
| :- | :-: | :- |
| `@enabled` | 0..1 | XPath expression determining the enabled (*true*) or disabled (*false*) state of subsequent *trace* operations. An omitted or empty value retains the current state.<br/><br/>At least one of `@enabled` or `@locked` is required. |
| `@locked` | 0..1 | XPath expression controlling whether the options can be changed (*false) or not (*true*) by subsequent requests. An omitted or empty value retains the current state.<br/><br/>At least one of `@enabled` or `@locked` is required. |
