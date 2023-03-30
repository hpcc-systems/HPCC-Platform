
### update-masking-context
```xml
    <update-masking-context>
        <set
            name="text literal"
            value="text literal"
            xpath_name="XPath evaluable to text"
            xpath_value="XPath evaluable to text"
        />
        <remove
            name="text literal"
            xpath_name="XPath evaluable to text"
        />
    </update-masking-context>
```

An operation that uses its children to manipulate the state of the current data masking context. If data masking is in use, custom masking properties (e.g., `valuetype-set` and `rule-set`) may be changed. Permitted changes include setting and removing custom properties.

The complete set of acceptable child elements is described here. Any other child element is considered an error.

This has no effect when data masking is not in use.

| Property | Count | Description |
| :- | :-: | :- |
| `remove` | 0..n | Declaration of one custom context property to be removed from the context. |
| `remove/@name` | 1..1 | Custom property identifier. |
| `remove/@xpath_name` | 0..1 | XPath evaluated to a custom property identifier.<br/><br/>Takes precedence over `remove/@name` and is required if that property is omitted or empty. |
| `set` | 0..n | Declaration of one custom context property to be set in the context. |
| `set/@name` | 1..1 | Custom property identifier. |
| `set/@value` | 1..1 | Custom property content. |
| `set/@xpath_name` | 0..1 | XPath evaluated to a custom property identifier.<br/><br/>Takes precedence over `set/@name` and is required if that property is omitted or empty. |
| `set/@xpath_value` | 0..1 | XPath evaluated to a custom property value.<br/><br/>Takes precedence over `set/@value` and is required if that property is omitted or empty. |
