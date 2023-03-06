
### tx-summary-timer
```xml
    <tx-summary-timer
        core_group="Boolean literal"
        level="enumerated text or numeric literal"
        mode="enumerated text literal"
        name="text literal">
      <!-- operations to be timed -->
    </tx-summary-timer>
```

The transaction summary is a list of significant data points related to a transaction. Upon transaction completion, one or more subsets of this list converted to text and sent to the ESP's trace log output. `tx-summary-timer` adds or updates a named value in the summary list with the total processing time, measured in milliseconds, of its child operations.

With respect to the summary, this operation is concerned only with the creation of summary data. With this in mind, summary data points may be one of two types:

- A *scalar* data point is a single, one-off, value with no inherent meaning to the ESP. Scalar values may be created depending on the `mode` property.
- A *timer* data point is a single value understood by the ESP to be a number that may be incremented after its insertion. Timer values may be created and/or updated depending on the `mode` propeerty.

| Property | Count | Description |
| :- | :-: | :- |
| core_group | 0..1 | Flag indicating whether the entry should (*true*) or should not (*false*) be included in output produced for the core audience. All entries are included in output for the enterprise audience.<br/><br/>Default when omitted or empty is *false*. |
| level | 0..1 | Minimum ESP log level at which the entry is included in log output. May be `min` (1), `normal` (5), `max` (10), or an integer between 1 and 10.<br/><br/>Default when omitted or empty is *1*. |
| mode | 0..1 | Choice of how to add the value to trace output. May be `append` (add a new scalar value to the end of the list), `set` (replace an existing scalar value or add a new scalar value to the end of the list), or `accumulate` (increment an existing timer value or add a new timer value to the end).<br/><br/>Default when omiited or empty is *append*. |
| name | 1..1 | Unique value label of the form `text ( '.' text )*`, where no `text` component may be empty.<br/><br/>The dotted name notation has special meaning in JSON formatted summary output. Each name segment preceding a dot represents the name of a JSON object nested within the summary object; multiple such segments result in multiple levels of embedded objects. |

Success is not guaranteed. Failure will produce warnings in trace output and script processing will continue. Script authors are responsible for resolving reported warnings. Reasons for failure are:

- Use of a malformed name;
- Appending a scalar with a name in use;
- Setting a scalar with a name in use as a timer;
- Accumulating a timer with a name in use as a scalar.

Avoid the following usage:
```xml
<tx-summary-timer name="my timer">
    <!-- some operations -->
    <tx-summary-timer name="my timer">
        <!-- some operations -->
    </tx-summary-timer>
    <!-- some operations -->
</tx-summary-timer>
```
The presumed intention of such markup is to time the execution of the outer operation, including the inner operation. The effect of this markup is to update the timer twice, once with the the duration of the inner operation and again with the duration of the outer operation (which already includes the inner operation's duration).
