#### getMaskValueBehavior
    number getMaskValueBehavior(valueType)
    number getMaskValueBehavior(valueType, maskStyle)

Determine how the current data masking context will respond to a `maskValue` request for a given value type and optional mask style. The result is a bit field that describes why masking will or will not occur.

- Bit 0 indicates if `valueType` is known in the profile.
- Bit 1 indicates if `maskStyle`, if given, is known in a selected value type (i.e., `valueType` if known or "\*" if not).
- Bit 2 indicates if masking will occur.

| Value | Bits | Meaning |
| :-: | :-: | :- |
| 0 | 000 | The value of `valueType` is not known and the unconditional masking value type named "\*" is not defined. A value will not be masked. |
| 1 | 001 | The value of `valueType` is known but unselected. The value of `maskStyle`, if given, is unknown. A value will not be masked. |
| 2 | 010 | Cannot occur. |
| 3 | 011 | The value of `valueType` is known but unselected. The value of `maskStyle` is known for the named type. A value will not be masked. |
| 4 | 100 | The value of `valueType` is unknown but the unconditional masking value type named "\*" is known and selected. A value will be masked. |
| 5 | 101 | The value of `valueType` is known and selected. The value of `maskStyle`, if given, is unknown. A value will be masked. |
| 6 | 110 | The value of `valueType` is unknown but the unconditional masking value type named "\*" is known and selected. The value of `maskStyle` is known by the unconditional value type. A value will be masked. |
| 7 | 111 | The value of `valueType` is known and selected. The value of `maskStyle` is known for the named type. A value will be masked. |

| Parameter | Required? | Description |
| :- | :-: | :- |
| valueType | Y | Identifier that may, but is not required to, be recognized as a masking profile value type. |
| maskStyle | N | Identifier that may, but is not required to, be recognized as a masking profile value type's mask style. Only relevant to the result if either the value of `valueType` is known in the profile or a value type named "\*" is known and selected in the profile. |
