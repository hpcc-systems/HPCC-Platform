#### canMaskContent
    boolean canMaskContent()
    boolean canMaskContent(contentType)

Determine how the current data masking context will respond to a `maskContent` request for an optional content type identifier. The result is a Boolean, *true* if at least one rule will be evaluated and *false* if no rules will be evaluated. A result of *true* does not imply that a masking request will apply any mask; it merely means that a mask could be applied given the *right* input content.

| Parameter | Required? | Description |
| :- | :-: | :- |
| contentType | N | Identifier that may, but is not required to, be recognized as a masking profile rule's content type. |
