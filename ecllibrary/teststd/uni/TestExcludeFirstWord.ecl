/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestExcludeFirstWord := MODULE

  EXPORT TestConst := MODULE
    //Check action on a string with no entries.
    EXPORT Test01 := ASSERT(Uni.ExcludeFirstWord(U'')+U'!' = U'!');
    EXPORT Test02 := ASSERT(Uni.ExcludeFirstWord(U'             ')+U'!' = U'!');
    //Check action on a string containing a single word - with various whitespace
    EXPORT Test03 := ASSERT(Uni.ExcludeFirstWord(U'x')+U'!' = U'!');
    EXPORT Test04 := ASSERT(Uni.ExcludeFirstWord(U' x')+U'!' = U'!');
    EXPORT Test05 := ASSERT(Uni.ExcludeFirstWord(U'x ')+U'!' = U'!');
    EXPORT Test06 := ASSERT(Uni.ExcludeFirstWord(U' x ')+U'!' = U'!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test07 := ASSERT(Uni.ExcludeFirstWord(U' abc def ')+U'!' = U'def !');
    EXPORT Test08 := ASSERT(Uni.ExcludeFirstWord(U'  a b c   def    ')+U'!' = U'b c   def    !');
    //Check action on a string containing multiple commas as part of a list initiated by a colon.
    EXPORT Test09 := ASSERT(Uni.ExcludeFirstWord(U' ,,,, ')+U'!' = U'!');
    EXPORT Test10 := ASSERT(Uni.ExcludeFirstWord(U'List: abc, def, ghi,')+U'!' = U'abc, def, ghi,!');
    //Check action on a string containing an apostrophe
    EXPORT Test11 := ASSERT(Uni.ExcludeFirstWord(U'Couldn\'t I?')+U'!' = U'I?!');
    //Check action on a string containing other Symbols
    EXPORT Test12 := ASSERT(Uni.ExcludeFirstWord(U'abc := name')+U'!' = U'name!');
    //Check action on a string containing different variations/combinations of numbers and other characters
    EXPORT Test13 := ASSERT(Uni.ExcludeFirstWord(U'123abc 234 1 23.6 abc123')+U'!' = U'234 1 23.6 abc123!');
    //Test other space characters (< 0x20)
    EXPORT Test14 := ASSERT(Uni.ExcludeFirstWord(U' b\nc \t   ')+U'!' = U'c \t   !');
    //Check action on a string containing latin diacritical marks
    EXPORT Test15 := ASSERT(Uni.ExcludeFirstWord(U'À à')+U'!' = U'à!');
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test16 := ASSERT(Uni.ExcludeFirstWord(U'Cambió las niñas')+U'!' = U'las niñas!');
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test17 := ASSERT(Uni.ExcludeFirstWord(U'我是電腦')+U'!' = U'電腦!');
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test18 := ASSERT(Uni.ExcludeFirstWord(U' Έρχεσαι συχνά εδώ; ')+U'!' = U'συχνά εδώ; !');
    //Testcases 19 and 20 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test19 := ASSERT(Uni.ExcludeFirstWord(U'صباح الخير')+U'!' = U'الخير!');
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test20 := ASSERT(Uni.ExcludeFirstWord(U'קָמָץ שִׁי״ן')+U'!' = U'שִׁי״ן!');
  END;

END;