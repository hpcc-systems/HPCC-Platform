
### trace-value
```xml
    <trace-value
        class="enumerated text literal"
        label="text literal"
        mask_style="text literal"
        select="XPath evaluable to text"
        test="XPath evaluable to Boolean"
        value_type="text literal"
        xpath_mask_style="XPath evaluable to text"
        xpath_value_type="XPath evaluable to text"/>
```

A childless ESDL script operation used to create a trace output entry using script generated content. If a data masking engine is configured, a masker has been selected prior to this operation, and `skip_mask` is not *true*, the generated content will be masked. If `select` evaluates to either a string value or a single element, the masker&apos;s `maskValue` method is invoked.

> Trace output is not generated and a warning is recorded if `select` resolves to multiple elements. This can only output a single value.

| Property | Count | Description |
| :- | :-: | :- |
| class | 0..1 | Choice of how the message should be classified, one of **error**, **warning**, **information**, or **progress**. Default when omitted or empty is **information**. |
| label | 0..1 | User defined prefix appearing before selected text. |
| mask_style | 0..1 | Choice of how an indidual data value should be masked. Should be empty or one of the mask style labels defined by the selected value type (see `value_type` or `xpath_value_type` for details on value type selection).<br/><br/>Only accepted when either `value_type` or `xpath_value_type` is given. Ignored when `xpath_mask_style` is given. |
| select | 1..1 | XPath expression that evaluates to the scalar value to be recorded. If the XPath evaluates to an XML node set, the scalar begins as the serialization of each XML node in the set. If the XPath evaluates to a value, the scalar begins as the evaluated value. The scalar may be modified by a selected data masking profile context if one is selected. |
| test | 0..1 | XPath expression that controls whether trace content should be generated. |
| value_type | 0..1 | Choice of how to interpret an individual data value. For example, is the value a password? The choice is not limited to value type names defined by the selected masker. If the masker enables unconditional masking, the choice should be limited to values for which masking may be expected. If the masker does not enable unconditional masking, the choice is not limited.<br/><br/>Ignored when `xpath_value_type` is given and required otherwise. |
| xpath_mask_style | 0..1 | Similar to `mask_style`, XPath evaluating to a choice of how an indidual data value should be masked. Should be empty or one of the mask style labels defined by the selected value type (see `value_type` or `xpath_value_type` for details on value type selection).<br/><br/>Only accepted when either `value_type` or `xpath_value_type` is given. Takes precedence over `mask_style` |
| xpath_value_type | 0..1 | Similar to `value_type`, XPath evaluating to a coice of how to interpret an individual data value. For example, is the value a password? The choice is not limited to value type names defined by the selected masker. If the masker enables unconditional masking, the choice should be limited to values for which masking may be expected. If the masker does not enable unconditional masking, the choice is not limited.<br/><br/>Takes precedence over `value_type`. The xpath *must* evaluate to a non-empty string when given. |

Trace output will be generated unless `@test` is present and evaluates to *false*. Neither the availability of data masking nor its ability to mask a particular value are prerequisites. Script authors are responsible for limiting the inclusion of restricted values, such as passwords or PII.

Future development will extend the ability to use, manipulate, and inspect masking capabilities:

- [Extend ESDL script syntax to mask data within an XPath](https://track.hpccsystems.com/browse/HPCC-28771)
- [Extend ESDL script syntax to interrogate data masking capabilities](https://track.hpccsystems.com/browse/HPCC-28772)
- [Extend ESDL script syntax to control the data masking environment](https://track.hpccsystems.com/browse/HPCC-28770)
