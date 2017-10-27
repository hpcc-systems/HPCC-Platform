/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestExcludeLastWord := MODULE

  EXPORT TestConst := MODULE
    //Check action on a string with no entries.
    EXPORT Test01 := ASSERT(Uni.ExcludeLastWord(U'')+U'!' = U'!');
    EXPORT Test02 := ASSERT(Uni.ExcludeLastWord(U'             ')+U'!' = U'!');
    //Check action on a string containing a single word - with various whitespace
    EXPORT Test03 := ASSERT(Uni.ExcludeLastWord(U'x')+U'!' = U'!');
    EXPORT Test04 := ASSERT(Uni.ExcludeLastWord(U' x')+U'!' = U'!');
    EXPORT Test05 := ASSERT(Uni.ExcludeLastWord(U'x ')+U'!' = U'!');
    EXPORT Test06 := ASSERT(Uni.ExcludeLastWord(U' x ')+U'!' = U'!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test07 := ASSERT(Uni.ExcludeLastWord(U' abc def ')+U'!' = U' abc !');
    EXPORT Test08 := ASSERT(Uni.ExcludeLastWord(U'  a b c   def    ')+U'!' = U'  a b c   !');
    //Check action on a string containing multiple commas as part of a list initiated by a colon.
    EXPORT Test09 := ASSERT(Uni.ExcludeLastWord(U' ,,,, ')+U'!' = U'!');
    EXPORT Test10 := ASSERT(Uni.ExcludeLastWord(U'List: abc, def, ghi,')+U'!' = U'List: abc, def, !');
    //Check action on a string containing an apostrophe
    EXPORT Test11 := ASSERT(Uni.ExcludeLastWord(U'I couldn\'t')+U'!' = U'I !');
    //Check action on a string containing other Symbols
    EXPORT Test12 := ASSERT(Uni.ExcludeLastWord(U'abc := name')+U'!' = U'abc := !');
    //Check action on a string containing different variations/combinations of numbers and other characters
    EXPORT Test13 := ASSERT(Uni.ExcludeLastWord(U'1 234 123abc 23.6 abc123')+U'!' = U'1 234 123abc 23.6 !');
    //Test other space characters (< 0x20)
    EXPORT Test14 := ASSERT(Uni.ExcludeLastWord(U'  a b\nc \t   ')+U'!' = U'  a b\n!');
    //Check action on a string containing latin diacritical marks
    EXPORT Test15 := ASSERT(Uni.ExcludeLastWord(U'À à')+U'!' = U'À !');
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test16 := ASSERT(Uni.ExcludeLastWord(U'El difunto cambió las niñas')+U'!' = U'El difunto cambió las !');
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test17 := ASSERT(((integer)Uni.Version() < 50) OR Uni.ExcludeLastWord(U'我是電腦')+U'!' = U'我是!'); // Chinese dictionary based iterators added in ICU 50
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test18 := ASSERT(Uni.ExcludeLastWord(U' Έρχεσαι συχνά εδώ; ')+U'!' = U' Έρχεσαι συχνά !');
    //Testcases 19 and 20 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test19 := ASSERT(Uni.ExcludeLastWord(U'صباح الخير')+U'!' = U'صباح !');
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test20 := ASSERT(Uni.ExcludeLastWord(U'קָמָץ שִׁי״ן')+U'!' = U'קָמָץ !');
  END;

END;