/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestGetNthWord := MODULE

  EXPORT TestConst := MODULE
    //Check action on a string with no entries.
    EXPORT Test01 := ASSERT(Uni.GetNthWord('',0)+'!' = '!', CONST);
    EXPORT Test02 := ASSERT(Uni.GetNthWord('',1)+'!' = '!', CONST);
    EXPORT Test03 := ASSERT(Uni.GetNthWord('',-1)+'!' = '!', CONST);
    EXPORT Test04 := ASSERT(Uni.GetNthWord('             ',0)+'!' = '!', CONST);
    EXPORT Test05 := ASSERT(Uni.GetNthWord('             ',1)+'!' = '!', CONST);
    EXPORT Test06 := ASSERT(Uni.GetNthWord('             ',-1)+'!' = '!', CONST);
    //Check action on a string containing a single word - with various whitespace
    EXPORT Test07 := ASSERT(Uni.GetNthWord('x',0)+'!' = '!');
    EXPORT Test08 := ASSERT(Uni.GetNthWord('x',1)+'!' = 'x!');
    EXPORT Test09 := ASSERT(Uni.GetNthWord('x',2)+'!' = '!');
    EXPORT Test10 := ASSERT(Uni.GetNthWord('x',3)+'!' = '!');
    EXPORT Test11 := ASSERT(Uni.GetNthWord(' x',1)+'!' = 'x!');
    EXPORT Test12 := ASSERT(Uni.GetNthWord('x ',1)+'!' = 'x!');
    EXPORT Test13 := ASSERT(Uni.GetNthWord(' x',2)+'!' = '!');
    EXPORT Test14 := ASSERT(Uni.GetNthWord(' x ',1)+'!' = 'x!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.GetNthWord(' abc def ', 1)+'!' = 'abc!');
    EXPORT Test16 := ASSERT(Uni.GetNthWord(' abc def ', 2)+'!' = 'def!');
    EXPORT Test17 := ASSERT(Uni.GetNthWord('  a b c   def    ',0)+'!' = '!');
    EXPORT Test18 := ASSERT(Uni.GetNthWord('  a b c   def    ',1)+'!' = 'a!');
    EXPORT Test19 := ASSERT(Uni.GetNthWord('  a b c   def    ',2)+'!' = 'b!');
    EXPORT Test20 := ASSERT(Uni.GetNthWord('  a b c   def    ',3)+'!' = 'c!');
    EXPORT Test21 := ASSERT(Uni.GetNthWord('  a b c   def    ',4)+'!' = 'def!');
    EXPORT Test22 := ASSERT(Uni.GetNthWord('  a b c   def    ',5)+'!' = '!');
    //Check action on a string containing multiple commas as part of a list initiated by a colon.
    EXPORT Test23 := ASSERT(Uni.GetNthWord(' ,,,, ',1)+'!' = ',,,,!');
    EXPORT Test24 := ASSERT(Uni.GetNthWord('List: abc, def, ghi,   jhi    ',0)+'!' = '!');
    EXPORT Test25 := ASSERT(Uni.GetNthWord('List: abc, def, ghi,   jhi    ',1)+'!' = 'List:!');
    EXPORT Test26 := ASSERT(Uni.GetNthWord('List: abc, def, ghi,   jhi    ',2)+'!' = 'abc,!');
    EXPORT Test27 := ASSERT(Uni.GetNthWord('List: abc, def, ghi,   jhi    ',4)+'!' = 'ghi,!');
    //Check action on a string containing an apostrophe
    EXPORT Test28 := ASSERT(Uni.GetNthWord('I couldn\'t hear you!',4)+'!' = 'you!!');
    EXPORT Test29 := ASSERT(Uni.GetNthWord('I couldn\'t hear you!',2)+'!' = 'couldn\'t!');
    //Check action on a string containing other Symbols
    EXPORT Test30 := ASSERT(Uni.GetNthWord('abc := name',1)+'!' = 'abc!');
    EXPORT Test31 := ASSERT(Uni.GetNthWord('abc := name',2)+'!' = 'name!');
    //Check action on a string containing different variations/combinations of numbers and other characters
    EXPORT Test32 := ASSERT(Uni.GetNthWord('1 234 123abc 23.6 abc123',1)+'!' = '1!');
    EXPORT Test33 := ASSERT(Uni.GetNthWord('1 234 123abc 23.6 abc123',2)+'!' = '234!');
    EXPORT Test34 := ASSERT(Uni.GetNthWord('1 234 123abc 23.6 abc123',3)+'!' = '123abc!');
    EXPORT Test35 := ASSERT(Uni.GetNthWord('1 234 123abc 23.6 abc123',4)+'!' = '23.6!');
    EXPORT Test36 := ASSERT(Uni.GetNthWord('1 234 123abc 23.6 abc123',5)+'!' = 'abc123!');
    //Test other space characters (< 0x20)
    EXPORT Test37 := ASSERT(Uni.GetNthWord('  a b\nc \t  def    ',2)+'!' = 'b!');
    EXPORT Test38 := ASSERT(Uni.GetNthWord('  a b\nc \t  def    ',3)+'!' = 'c!');
    //Check action on a string containing latin diacritical marks
    EXPORT Test39 := ASSERT(Uni.GetNthWord(U'À à',1)+U'!' = U'À!');
    EXPORT Test40 := ASSERT(Uni.GetNthWord(U'ä̰́ Ä̰́',1)+U'!' = U'ä̰́!');
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls" --> "changed" & --> "girls"
    EXPORT Test41 := ASSERT(Uni.GetNthWord(U'El difunto cambió las niñas',3)+U'!' = U'cambió!');
    EXPORT Test42 := ASSERT(Uni.GetNthWord(U'El difunto cambió las niñas',5)+U'!' = U'niñas!');
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer" --> "computer"
    EXPORT Test43 := ASSERT(Uni.GetNthWord(U'我是電腦',2)+U'!' = U'電腦!');
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?" --> "often"
    EXPORT Test44 := ASSERT(Uni.GetNthWord(U' Έρχεσαι συχνά εδώ; ',2)+U'!' = U'συχνά!');
    //Testcases 45 and 46 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning" --> "morning"
    EXPORT Test45 := ASSERT(Uni.GetNthWord(U'صباح الخير',1)+U'!' = U'صباح!');
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test46 := ASSERT(Uni.GetNthWord(U'קָמָץ שִׁי״ן',1)+U'!' = U'קָמָץ!');
  END;

END;
