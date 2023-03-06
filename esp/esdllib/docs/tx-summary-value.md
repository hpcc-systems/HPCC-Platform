
### tx-summary-value
```xml
    <tx-summary-value
        core_group="Boolean literal"
        level="enumerated text or numeric literal"
        mode="enumerated text literal"
        name="text literal"
        select="XPath evaluable to text"
        type="enumerated text literal"/>
```

The transaction summary is a list of significant data points related to a transaction. Upon transaction completion, one or more subsets of this list are converted to text and sent to the ESP's trace log output. `tx-summary-value` adds or updates a named value in the summary list with the result of evaluating the `value` XPath.

With respect to the summary, this childless operation is concerned only with the creation of summary data. With this in mind, summary data points may be one of two types:

- A *scalar* data point is a single, one-off, value with no inherent meaning to the ESP. All values created are scalars.
- A *timer* data point is a single value understood by the ESP to be a number that may be incremented after its insertion. No timer values are created or updated.

| Property | Count | Description |
| :- | :-: | :- |
| core_group | 0..1 | Flag indicating whether the entry should (*true*) or should not (*false*) be included in output produced for the core audience. All entries are included in output for the enterprise audience.<br/><br/>Default when omitted or empty is *false*. |
| level | 0..1 | Minimum ESP log level at which the entry is included in log output. May be `min` (1), `normal` (5), `max` (10), or an integer between 1 and 10.<br/><br/>Default when omitted or empty is *1*. |
| mode | 0..1 | Choice of how to add the value to trace output. May be `append` (add a new scalar value to the end of the list) or `set` (replace an existing scalar value or add a new scalar value to the end of the list).<br/><br/>Default when omiited or empty is *append*. |
| name | 1..1 | Unique value label of the form `text ( '.' text )*`, where no `text` component may be empty.<br/><br/>The dotted name notation has special meaning in JSON formatted summary output. Each name segment preceding a dot represents the name of a JSON object nested within the summary object; multiple such segments result in multiple levels of embedded objects. |
| select | 1..1 | XPath expression that evaluates to the scalar value to be recorded. |
| type | 0..1 | Choice of how to interpret the evaluated `select` expression. May be *text* if the value should be used as-is. May be *signed* if the value should be handled as a signed integer, up to 64 bits. May be *unsigned* if the value should be handled as an unsigned integer, up to 64 bits. May be *decimal* if the value should be handled as a non-integral number.<br/><br/>Default when omitted or empty is *text*. The default can handle all numeric values, but numbers will be presented as strings in JSON output. The default must be overridden when JSON output requires typed numeric values. |

Success is not guaranteed. Failure will produce warnings in trace output and script processing will continue. Script authors are responsible for resolving reported warnings. Reasons for failure are:

- Use of a malformed name;
- Appending a scalar with a name in use;
- Setting a scalar with a name in use as a timer.
