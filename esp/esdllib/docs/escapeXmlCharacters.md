#### escapeXmlCharacters
    string escapeXmlCharacters(xml)

Return a copy of the input data with XML-restricted characters replaced by character entities. For example, `<` is replaced with `&lt;` in the output. Encoding an encoded value, encoding the ampersands, is supported and may be desirable (see below).

| Parameter | Required? | Description |
| :- | :-: | :- |
| xml | Y | Character buffer that may include XML markup. May be empty. |

##### Known Issue(s)

###### HPCC-28673

https://track.hpccsystems.com/browse/HPCC-28673

Insertion of content containing XML-restricted characters into the script context as element content does not behave as expected. Given operations `<es:set-value target="unencoded" select="'<tag/>'"/>` and `<es:set-value target="encodeded" select="'&lt;tag/&gt;')"/>`, both `unencoded` and `encoded` evaluate to `<tag/>`, when `encoded` is expected to be `&lt;tag/&gt;`.

Double escaping (i.e., `escapeXmlCharacters(escapeXmlCharacters('<tag/>'))`) provides a limited workaround. Manipulation of the value by itself is addressed. If the value is included in a larger fragment of markup being manipulated, the value within the fragment will no longer be double-escaped.

What this means is that an operation like `<es:set-value target="here" select="toXmlString('.')"/>` will lose any escaped characters when updating the content of `here`. Double escaping a value somewhere in the current node will lose one level of escaping when the XML string is assembled, and will lose the second level of escaping when inserted at `here`. A value that begins as escaped markup becomes unescaped markup at the end.
