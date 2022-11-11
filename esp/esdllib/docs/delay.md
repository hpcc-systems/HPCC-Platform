### delay
```xml
    <delay
        millis="unsigned integer literal"/>
```

For testing purposes, it can be helpful to have operations with predictable durations. Consider `tx-summary-timer` recording elapsed time. Testing the timer requires waiting for predictable periods of time.

The delay is guaranteed to be at least `millis` milliseonds in duration, as measured in a difference in tick counts. It is not guarantedd to be exactly `millis` milliseonds in duration; no attempt is made to limit the duration.

| Property | Count | Description |
| :- | :-: | :- |
| millis | 0..1 | The requested number of milliseconds difference between ending and beginning tick counts.<br/><br/>Default when omitted or empty is *1*. |
