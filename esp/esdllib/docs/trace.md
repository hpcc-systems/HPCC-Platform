
### trace
```xml
    <trace
        content_type="enumerated text literal"
        label="text literal"
        select="XPath evaluable to text"
        skip_mask="XPath evaluable to Boolean"
        test="XPath evaluable to Boolean"/>
```

A childless ESDL script operation used to create a trace output entry using script generated content. If a data masking engine is configured and a profile context has been selected prior to this operation, the generated content will be masked.

| Property | Count | Description |
| :- | :-: | :- |
| content_type | 0..1 | Choice of how a data masking profile may filter the rules applied when masking content. May be empty, which requests not filter. Should be one of the content type identifiers present in data masking profiles, which requests rules be filtered by the identifier.<br/><br/>For example, if your profiles use both *xml* and *json* to describe filters for XML and JSON content, then this would be a choice of empty, *xml*, or *json*. |
| label | 0..1 | User defined prefix appearing be selected text. |
| select | 1..1 | XPath expression that evaluates to the scalar value to be recorded. If the XPath evaluates to an XML node set, the scalar begins as the serialization of each XML node in the set. If the XPath evaluates to a value, the scalar begins as the evaluated value. The scalar may be modified by a selected data masking profile context if one is selected. |
| skip_mask | 0..1 | XPath expression that controls whether generated data bypasses configured data masking. Masking is bypassed only when this expression evaluates to *true*.<br/><br/>Masking content can be a time consuming process. If the selected data is known to contain no values to be masked, the performance impact can be avoided with this property. The property should be omitted or evaluate to *false* if there is any chance the data contains a value requiring masking. |
| test | 0..1 | XPath expression that controls whether trace content should be generated. |

Trace output will be generated unless `@test` is present and evaluates to *false*. Neither the availability of data masking nor its ability to mask a particular value are prerequisites. Script authors are responsible for limiting the inclusion of restricted values, such as passwords or PII.

Future development will extend the ability to use, manipulate, and inspect masking capabilities:

- [Extend ESDL script trace operation syntax to mask individual values](https://track.hpccsystems.com/browse/HPCC-28770)
- [Extend ESDL script syntax to mask data within an XPath](https://track.hpccsystems.com/browse/HPCC-28771)
- [Extend ESDL script syntax to interrogate data masking capabilities](https://track.hpccsystems.com/browse/HPCC-28772)
- [Extend ESDL script syntax to control the data masking environment](https://track.hpccsystems.com/browse/HPCC-28770)
