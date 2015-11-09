rec := record
  unsigned8 id;
  unicode   searchText;
end;

cleansedFieldInline := dataset([{6420, ''}], rec);

pattern words := pattern('[^,;]+');
pattern sepchar := [',',';','AND'];
rule termsRule := FIRST words sepchar |sepchar words LAST | sepchar words sepchar | FIRST words LAST;

normalizeSeperators(unicode str) := regexreplace(u'AND',str,u',');

termsDs := parse(NOFOLD(cleansedFieldInline),
                 normalizeSeperators(searchText),
                 termsRule,
                 transform({rec, unicode terms},
                           self.terms := trim(matchunicode(words),left,right),
                           self := left),
                 SCAN ALL);

sequential (
  output(termsDs);  // Test parsing an empty string with a pattern that has a minimum match length > 0
);
