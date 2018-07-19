/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestSplitWords := MODULE
  EXPORT TestRuntime := MODULE
    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.SplitWords(U'', U'') = []);
    EXPORT Test01b := ASSERT(Uni.SplitWords(U'abcde', U'') = [U'abcde']);
    EXPORT Test01c := ASSERT(Uni.SplitWords(U'', U'abc', TRUE) = []);
    EXPORT Test02 := ASSERT(Uni.SplitWords(U'x', U'x') = []);
    EXPORT Test03 := ASSERT(Uni.SplitWords(U'x', U' ') = [U'x']);
    EXPORT Test04 := ASSERT(Uni.SplitWords(U' ', U' ') = []);
    EXPORT Test05 := ASSERT(Uni.SplitWords(U'  ', U' ') = []);
    EXPORT Test06 := ASSERT(Uni.SplitWords(U'x ', U' ') = [U'x']);
    EXPORT Test07 := ASSERT(Uni.SplitWords(U' x', U' ') = [U'x']);
    EXPORT Test08 := ASSERT(Uni.SplitWords(U' x ', U' ') = [U'x']);
    EXPORT Test09 := ASSERT(Uni.SplitWords(U' abc def ', U' ') = [U'abc',U'def']);
    EXPORT Test10 := ASSERT(Uni.SplitWords(U' abc   def ', U' ') = [U'abc',U'def']);
    EXPORT Test11 := ASSERT(Uni.SplitWords(U' a b c   def ', U' ') = [U'a', U'b', U'c',U'def']);
    EXPORT Test12 := ASSERT(Uni.SplitWords(U' abc   def', U' ') = [U'abc',U'def']);
    EXPORT Test13 := ASSERT(Uni.SplitWords(U'$', U'$$') = [U'$']);
    EXPORT Test14 := ASSERT(Uni.SplitWords(U'$x', U'$$') = [U'$x']);
    EXPORT Test15 := ASSERT(Uni.SplitWords(U'$$', U'$$') = []);
    EXPORT Test16 := ASSERT(Uni.SplitWords(U'$$$', U'$$') = [U'$']);
    EXPORT Test17 := ASSERT(Uni.SplitWords(U'$$$$', U'$$') = []);
    EXPORT Test18 := ASSERT(Uni.SplitWords(U'$$x$$', U'$$') = [U'x']);
    EXPORT Test19 := ASSERT(Uni.SplitWords(U'$$x$$y', U'$$') = [U'x',U'y']);
    EXPORT Test21 := ASSERT(Uni.SplitWords(U'a,c,d', U',', TRUE) = [U'a',U'c',U'd']);
    EXPORT Test21a := ASSERT(Uni.SplitWords(U'a,c,d', U',', FALSE) = [U'a',U'c',U'd']);
    EXPORT Test22 := ASSERT(Uni.SplitWords(U'a,,d', U',', TRUE) = [U'a',U'',U'd']);
    EXPORT Test22a := ASSERT(Uni.SplitWords(U'a,,d', U',', FALSE) = [U'a',U'd']);
    EXPORT Test23 := ASSERT(Uni.SplitWords(U',,,', U',', TRUE) = [U'',U'',U'',U'']);
    EXPORT Test23a := ASSERT(Uni.SplitWords(U',,,', U',', FALSE) = []);
    EXPORT Test24 := ASSERT(Uni.SplitWords(U' \377ABCDEF FEDCBA ', U' ') = [U'\377ABCDEF',U'FEDCBA']);
    //Check action on a string containing punctuation characters.
    EXPORT Test25 := ASSERT(Uni.SplitWords(U' ,&%$@ ',U'%$') = [U' ,&',U'@ ']);
    //Check action on a string containing an apostrophe.
    EXPORT Test26 := ASSERT(Uni.SplitWords(U'I couldn\'t hear you!',U'\'') = [U'I couldn',U't hear you!']);
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test27 := ASSERT(Uni.SplitWords(U'1 234 123abc 23.6 abc123',U'2') = [U'1 ',U'34 1',U'3abc ',U'3.6 abc1',U'3']);
    //Test other space characters (< 0x20).
    EXPORT Test28 := ASSERT(Uni.SplitWords(U'an\nt\tdef',U' ') = [U'an\nt\tdef']);
    EXPORT Test29 := ASSERT(Uni.SplitWords(U'  a n\nt \t  def    ',U't') = [U'  a n\n',U' \t  def    ']);
    //Check action on a string containing latin diacritical marks.
    EXPORT Test30 := ASSERT(Uni.SplitWords(U'À à',U'À') = [U' à']);
    EXPORT Test31 := ASSERT(Uni.SplitWords(U'ȭ š',U'ȭ') = [U' š']);
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test32 := ASSERT(Uni.SplitWords(U'El difunto cambió las niñas',U'cambió') = [U'El difunto ',U' las niñas']);
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test33 := ASSERT(Uni.SplitWords(U'我是電腦',U'是') = [U'我',U'電腦']);
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test34 := ASSERT(Uni.SplitWords(U' Έρχεσαι συχνά εδώ; ',U'χ') = [U' Έρ',U'εσαι συ',U'νά εδώ; ']);
    //Testcases 35 and 36 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test35 := ASSERT(Uni.SplitWords(U'صباح الخير',U'ا') = [U'صب',U'ح ',U'لخير']);
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test36 := ASSERT(Uni.SplitWords(U'קָמָץ שִׁי״ן',U'קָ') = [U'מָץ שִׁי״ן']);
    //Check action on surrogate pairs.
    EXPORT Test37 := ASSERT(Uni.SplitWords(U'x𐐀x𐐀',U'𐐀') = [U'x',U'x']);
    EXPORT Test38 := ASSERT(Uni.SplitWords(U'𐐀',U'𐐀') = []);
    EXPORT Test39 := ASSERT(Uni.SplitWords(U'x',U'𐐀') = [U'x']);
    EXPORT Test40 := ASSERT(Uni.SplitWords(U'𐐀xx𐐀𐐀',U'x') = [U'𐐀',U'𐐀𐐀']);
  END;
END;
