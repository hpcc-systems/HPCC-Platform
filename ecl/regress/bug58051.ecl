stringLayout := {
   string s1,
};

unicodeLayout := {
   unicode u1,
};

dsString := dataset([{'ab'}], stringLayout);
dsUnicode := dataset([{u'ab'}], unicodeLayout);

pattern nameLetter   :=  any;
pattern namePattern := (nameLetter+);
pattern namePattern2 := (nameLetter+);

pattern parsePattern := [namePattern, namePattern2];

stringLayout parseStringPattern(stringLayout rec) := transform
   s1 :=  matchtext(parsePattern);
   self.s1 := s1 + '\n';
end;

unicodeLayout parseUnicodePattern(unicodeLayout rec) := transform
   u1 :=  matchunicode(parsePattern);
   self.u1 := u1 + U'\n';
end;

parsedDatasetString := parse(dsString,
                       s1,
                       parsePattern,
                       parseStringPattern(left));

parsedDatasetUnicode := parse(dsUnicode,
                       u1,
                       parsePattern,
                       parseUnicodePattern(left));

output(parsedDatasetString, named('string'));
// ab
// a
// b

output(parsedDatasetUnicode, named('unicode'));
// ab
// a
// ab
// a
// b
// b
