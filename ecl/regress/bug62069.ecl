stringLayout := {
   string s1,
};

unicodeLayout := {
   unicode u1,
};

dsString := dataset([{'abc'},{'defghijklm'}], stringLayout);
dsUnicode := dataset([{u'abc'},{u'defghijklm'}], unicodeLayout);

pattern letter := pattern('[^<]')+;

token tokenLetter := letter;

stringLayout parseStringPattern(stringLayout rec) := transform
   s1 :=  matchtext(tokenLetter);
   self.s1 := s1+'\n';
end;

unicodeLayout parseUnicodePattern(unicodeLayout rec) := transform
   u1 :=  matchunicode(tokenLetter);
   self.u1 := u1+U'\n';
end;

parsedDatasetString := parse(dsString,
                             s1,
                             tokenLetter,
                             parseStringPattern(left),
                             maxlength(4));

parsedDatasetUnicode := parse(dsUnicode,
                              u1,
                              tokenLetter,
                              parseUnicodePattern(left),
                              maxlength(4));

output(parsedDatasetString, named('string'));
// MAXLENGTH = 2
// bc
// lm

// MAXLENGTH = 4
// abc
// jklm

output(parsedDatasetUnicode, named('unicode'));
// MAXLENGTH = 2
// ab
// c
// de
// fg
// hi
// jk
// lm

// MAXLENGTH = 4
// abc
// def
// ghi
// jkl
// m
