#### decodeXmlText
    string decodeXmlText(xml)

Return a copy of the input data with XML character entities replaced by the characters they represent. For example, `&lt;` is replaced by `<` in the output.

| Parameter | Required? | Description |
| :- | :-: | :- |
| xml | Y | Character buffer containing text presumed to include XML markup with XML restricted characters replaced by character entities. May be empty. |
