/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestWordCount := MODULE

  EXPORT TestConst := MODULE
    //Check action on a string with no entries.
    EXPORT Test01 := ASSERT(Uni.WordCount('') = 0);
    EXPORT Test02 := ASSERT(Uni.WordCount('             ') = 0);
    //Check action on a string containing a single word - with various whitespace
    EXPORT Test03 := ASSERT(Uni.WordCount('x') = 1);
    EXPORT Test04 := ASSERT(Uni.WordCount(' x') = 1);
    EXPORT Test05 := ASSERT(Uni.WordCount('x ') = 1);
    EXPORT Test06 := ASSERT(Uni.WordCount(' x ') = 1);
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test07 := ASSERT(Uni.WordCount(' abc def ') = 2);
    EXPORT Test08 := ASSERT(Uni.WordCount('  a b c   def    ') = 4);
    //Check action on a string containing multiple commas as part of a list initiated by a colon.
    EXPORT Test09 := ASSERT(Uni.WordCount(' ,,,, ') = 0);
    EXPORT Test10 := ASSERT(Uni.WordCount('List: abc, def, ghi,   jhi    ') = 5);
    //Check action on a string containing an apostrophe
    EXPORT Test11 := ASSERT(Uni.WordCount('I couldn\'t hear you!') = 4);
    //Check action on a string containing other Symbols
    EXPORT Test12 := ASSERT(Uni.WordCount('abc := name') = 2);
    //Check action on a string containing different variations/combinations of numbers and other characters
    EXPORT Test13 := ASSERT(Uni.WordCount('1 234 123abc 23.6 abc123') = 5);
    //Test other space characters (< 0x20)
    EXPORT Test14 := ASSERT(Uni.WordCount('  a b\nc \t  def    ') = 4);
    //Check action on a string containing latin diacritical marks
    EXPORT Test15 := ASSERT(Uni.WordCount(U'À à') = 2);
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test16 := ASSERT(Uni.WordCount(U'El difunto cambió las niñas') = 5);
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test17 := ASSERT(Uni.WordCount(U'我是電腦') = 2);
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test18 := ASSERT(Uni.WordCount(U' Έρχεσαι συχνά εδώ; ') = 3);
    //Testcases 19 and 20 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test19 := ASSERT(Uni.WordCount(U'صباح الخير') = 2);
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test20 := ASSERT(Uni.WordCount(U'קָמָץ שִׁי״ן') = 2);
  END;

END;
