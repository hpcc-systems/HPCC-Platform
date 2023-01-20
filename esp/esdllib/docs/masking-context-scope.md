
### masking-context-scope
```xml
    <masking-context-scope>
      <!-- affected child operations -->
    </masking-context-scope>
```

An operation with children, creates a temporary scope within which changes to data masking settings may be applied. All changes made by `update-masking-context` while processing an instance of this operation are discarded when processing completes.

| Property | Count | Description |
| :- | :-: | :- |
| N/A | 0..0 | No operation-specific properties are defined. |
