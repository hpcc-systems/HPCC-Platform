#### getTxSummary
    string getTxSummary()
    string getTxSummary(style)
    string getTxSummary(style, level)
    string getTxSummary(style, level, group)

The transaction summary is a list of significant data points related to a transaction. Upon transaction completion, one or more subsets of this list are converted to text and sent to the ESP's trace log output. `getTxSummary` converts the current contents of the summary to text that can be manipulated by other script components.

| Parameter | Required? | Description |
| :- | :-: | :- |
| style | N | Text selector identifying the output text format. May be *text* for legacy text format, or *json* for JSON format.<br/><br/>Default when omitted or empty is *text*. |
| level | N | Text or numeric filter limiting included summary values based on log level. May be *min* for LogMin, *normal* for LogNormal, *max* for LogMax, or an integer between LogMin (1) and LogMax (10).<br/><br/>Default when omitted or empty is LogMin. |
| group | N | Text filter limiting included summary values based on target audience. May be *core* for standard platform values. May be *enterprise* for non-standard platform and script-defined values.<br/><br/>Default when omitted or empty is *enterprise*. |

##### Examples

###### Default Summary

This script operation:

```xml
<es:set-value target="TxSummary" select="getTxSummary()"/>
```

Yields content similar to this:

```xml
<TxSummary>activeReqs=1</TxSummary>
```

###### JSON Summary

Similar to the default summary above, this operation:

```xml
<es:set-value target="TxSummary" select="getTxSummary('json')"/>
```

Yields content similar to this:

```xml
<TxSummary>{"activeReqs": 1}</TxSummary>
```
