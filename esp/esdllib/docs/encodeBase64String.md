#### encodeBase64String
    string encodeBase64String(text)
    string encodeBase64String(text, includeLineBreaks)

Apply the Base64 encoder to an input buffer and return the encoded text.

| Parameter | Required? | Description |
| :- | :-: | :- |
| text | Y | Character buffer to encoded. May be empty. |
| includeLineBreaks | N | Boolean indicating whether the encoded output should include line breaks (*true*) or not (*false*).<br/><br/>Default when omitted is *false*. |
