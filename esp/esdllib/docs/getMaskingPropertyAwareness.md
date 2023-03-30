#### getMaskingPropertyAwareness
    number getMaskingPropertyAwareness(name)

Determine the current data masking context's support for a particular custom property.

- 0: the property is not supported.
- 1: the property is recognized by the context, but use of the property will not affect the outcome of masking requests.
- 2: use of the property can affect the outcome of masking requests.

| Parameter | Required? | Description |
| :- | :-: | :- |
| name | Y | Identifier that may, but is not required to, be recognized as a masking profile's custom property. |
