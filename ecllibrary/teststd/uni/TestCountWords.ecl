/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestCountWords := MODULE

  EXPORT TestConst := MODULE
    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.CountWords(U'', U'') = 0);
    EXPORT Test01b := ASSERT(Uni.CountWords(U'abcde', U'') = 1);
    EXPORT Test01c := ASSERT(Uni.CountWords(U'', U'abc', TRUE) = 0);
    EXPORT Test02 := ASSERT(Uni.CountWords(U'x', U'x') = 0);
    EXPORT Test03 := ASSERT(Uni.CountWords(U'x', U' ') = 1);
    EXPORT Test04 := ASSERT(Uni.CountWords(U' ', U' ') = 0);
    EXPORT Test05 := ASSERT(Uni.CountWords(U'  ', U' ') = 0);
    EXPORT Test06 := ASSERT(Uni.CountWords(U'x ', U' ') = 1);
    EXPORT Test07 := ASSERT(Uni.CountWords(U' x', U' ') = 1);
    EXPORT Test08 := ASSERT(Uni.CountWords(U' x ', U' ') = 1);
    EXPORT Test09 := ASSERT(Uni.CountWords(U' abc def ', U' ') = 2);
    EXPORT Test10 := ASSERT(Uni.CountWords(U' abc   def ', U' ') = 2);
    EXPORT Test11 := ASSERT(Uni.CountWords(U' a b c   def ', U' ') = 4);
    EXPORT Test12 := ASSERT(Uni.CountWords(U' abc   def', U' ') = 2);
    EXPORT Test13 := ASSERT(Uni.CountWords(U'$', U'$$') = 1);
    EXPORT Test14 := ASSERT(Uni.CountWords(U'$x', U'$$') = 1);
    EXPORT Test15 := ASSERT(Uni.CountWords(U'$$', U'$$') = 0);
    EXPORT Test16 := ASSERT(Uni.CountWords(U'$$$', U'$$') = 1);
    EXPORT Test17 := ASSERT(Uni.CountWords(U'$$$$', U'$$') = 0);
    EXPORT Test18 := ASSERT(Uni.CountWords(U'$$x$$', U'$$') = 1);
    EXPORT Test19 := ASSERT(Uni.CountWords(U'$$x$$y', U'$$') = 2);
    EXPORT Test20 := ASSERT(Uni.CountWords(U'$$x$$xy', U'$$') = 2);
    EXPORT Test21 := ASSERT(Uni.CountWords(U'a,c,d', U',', TRUE) = 3);
    EXPORT Test21a := ASSERT(Uni.CountWords(U'a,c,d', U',', FALSE) = 3);
    EXPORT Test22 := ASSERT(Uni.CountWords(U'a,,d', U',', TRUE) = 3);
    EXPORT Test22a := ASSERT(Uni.CountWords(U'a,,d', U',', FALSE) = 2);
    EXPORT Test23 := ASSERT(Uni.CountWords(U',,,', U',', TRUE) = 4);
    EXPORT Test23a := ASSERT(Uni.CountWords(U',,,', U',', FALSE) = 0);
    EXPORT Test24 := ASSERT(Uni.CountWords(U' \377ABCDEF FEDCBA ', U' ') = 2);
    //Check action on a string containing punctuation characters.
    EXPORT Test25 := ASSERT(Uni.CountWords(U' ,&%$@ ',U'%$') = 2);
    //Check action on a string containing an apostrophe.
    EXPORT Test26 := ASSERT(Uni.CountWords(U'I couldn\'t hear you!',U'\'') = 2);
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test27 := ASSERT(Uni.CountWords(U'1 234 123abc 23.6 abc123',U'2') = 5);
    //Test other space characters (< 0x20).
    EXPORT Test28 := ASSERT(Uni.CountWords(U'an\nt\tdef',U' ') = 1);
    EXPORT Test29 := ASSERT(Uni.CountWords(U'  a n\nt \t  def    ',U't') = 2);
    //Check action on a string containing latin diacritical marks.
    EXPORT Test30 := ASSERT(Uni.CountWords(U'À à',U'À') = 1);
    EXPORT Test31 := ASSERT(Uni.CountWords(U'ȭ š',U'ȭ') = 1);
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test32 := ASSERT(Uni.CountWords(U'El difunto cambió las niñas',U'cambió') = 2);
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test33 := ASSERT(Uni.CountWords(U'我是電腦',U'是') = 2);
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test34 := ASSERT(Uni.CountWords(U' Έρχεσαι συχνά εδώ; ',U'χ') = 3);
    //Testcases 35 and 36 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test35 := ASSERT(Uni.CountWords(U'صباح الخير',U'ا') = 3);
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test36 := ASSERT(Uni.CountWords(U'קָמָץ שִׁי״ן',U'קָ') = 1);
    //Check action on surrogate pairs.
    EXPORT Test37 := ASSERT(Uni.CountWords(U'x𐐀x𐐀',U'𐐀') = 2);
    EXPORT Test38 := ASSERT(Uni.CountWords(U'𐐀',U'𐐀') = 0);
    EXPORT Test39 := ASSERT(Uni.CountWords(U'x',U'𐐀') = 1);
    EXPORT Test40 := ASSERT(Uni.CountWords(U'𐐀xx𐐀𐐀',U'x') = 2);
  END;

END;
