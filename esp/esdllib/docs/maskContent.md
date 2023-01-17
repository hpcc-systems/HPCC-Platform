#### maskContent
    string maskContent(content)
    string maskContent(content, contentType)

Apply the currently configured data masking context to potentially mask substrings of `content` based on defined rules. The input buffer is assumed to contain contextual clues mapping substrings to be masked to profile rules. Masking only occurs if these clues are located.

For example, assume the current context defines a clue that a password value can be found between the text strings `<Password>` and `</Password>`. Applying content masking to the buffer `...<Password/>my password</Password>...` will mask the substring `my password`. Applying content masking to the buffer `...my password is "my password"...` will not mask either `my password` substring because the required clues are missing.

Managing selected rules is handled by manipulating the **rule-set** custom profile property. Valid values for the property include "" (the empty string) and other masking profile-defined identifiers that are beyond the scope of this document.

| Parameter | Required? | Description |
| :- | :-: | :- |
| content | Y | The text input to be searched for embedded values. The buffer may contain any number of embedded values. |
| contentType | N | A masking profile-defined rule filter. Rules may be assigned a content type identifier. Masking with a content type limits masking to only those rules assigned a matching identifier or assigned no identifier at all.<br/></br>Assume three rules are defined, with one applicable to serialized XML markup and with an *xml* identifier, one applicable to serialized JSON markup and with a "json" identifier, and one ambiguous with no identifer. A caller that understands, for example, that the text to be searched is serialized XML markup may exclude rules identified as "json" for improved performance. |
