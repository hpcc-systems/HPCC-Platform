/*##############################################################################
     HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
 
     Licensed under the Apache License, Version 2.0 (the "License");
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at
 
        http://www.apache.org/licenses/LICENSE-2.0
 
     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
 ##############################################################################*/
 
 IMPORT Std.Uni;
 
 EXPORT TestExcludeNthWord := MODULE
 
   EXPORT TestConst := MODULE
    //Check action on a string with no entries.
    EXPORT Test01 := ASSERT(Uni.ExcludeNthWord('',0)+'!' = '!', CONST);
    EXPORT Test02 := ASSERT(Uni.ExcludeNthWord('',1)+'!' = '!', CONST);
    EXPORT Test03 := ASSERT(Uni.ExcludeNthWord('',-1)+'!' = '!', CONST);
    EXPORT Test04 := ASSERT(Uni.ExcludeNthWord('             ',0)+'!' = '!', CONST);
    EXPORT Test05 := ASSERT(Uni.ExcludeNthWord('             ',1)+'!' = '!', CONST);
    EXPORT Test06 := ASSERT(Uni.ExcludeNthWord('             ',-1)+'!' = '!', CONST);
    //Check action on a string containing a single word - with various whitespace
    EXPORT Test07 := ASSERT(Uni.ExcludeNthWord('x',0)+'!' = 'x!');
    EXPORT Test08 := ASSERT(Uni.ExcludeNthWord('x',1)+'!' = '!');
    EXPORT Test09 := ASSERT(Uni.ExcludeNthWord('x',2)+'!' = 'x!');
    EXPORT Test10 := ASSERT(Uni.ExcludeNthWord('x',3)+'!' = 'x!');
    EXPORT Test11 := ASSERT(Uni.ExcludeNthWord(' x',1)+'!' = '!');
    EXPORT Test12 := ASSERT(Uni.ExcludeNthWord('x ',1)+'!' = '!');
    EXPORT Test13 := ASSERT(Uni.ExcludeNthWord(' x',2)+'!' = ' x!');
    EXPORT Test14 := ASSERT(Uni.ExcludeNthWord(' x ',1)+'!' = '!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.ExcludeNthWord(' abc def ', 1)+'!' = 'def !');
    EXPORT Test16 := ASSERT(Uni.ExcludeNthWord(' abc def ', 2)+'!' = ' abc !');
    EXPORT Test17 := ASSERT(Uni.ExcludeNthWord('  a b c   def    ',0)+'!' = '  a b c   def    !');
    EXPORT Test18 := ASSERT(Uni.ExcludeNthWord('  a b c   def    ',1)+'!' = 'b c   def    !');
    EXPORT Test19 := ASSERT(Uni.ExcludeNthWord('  a b c   def    ',2)+'!' = '  a c   def    !');
    EXPORT Test20 := ASSERT(Uni.ExcludeNthWord('  a b c   def    ',3)+'!' = '  a b def    !');
    EXPORT Test21 := ASSERT(Uni.ExcludeNthWord('  a b c   def    ',4)+'!' = '  a b c   !');
    EXPORT Test22 := ASSERT(Uni.ExcludeNthWord('  a b c   def    ',5)+'!' = '  a b c   def    !');
    //Check action on a string containing multiple commas as part of a list initiated by a colon.
    EXPORT Test23 := ASSERT(Uni.ExcludeNthWord(' ,,,, ',1)+'!' = '!');
    EXPORT Test24 := ASSERT(Uni.ExcludeNthWord('List: abc, def, ghi,   jhi    ',0)+'!' = 'List: abc, def, ghi,   jhi    !');
    EXPORT Test25 := ASSERT(Uni.ExcludeNthWord('List: abc, def, ghi,   jhi    ',1)+'!' = 'abc, def, ghi,   jhi    !');
    EXPORT Test26 := ASSERT(Uni.ExcludeNthWord('List: abc, def, ghi,   jhi    ',2)+'!' = 'List: def, ghi,   jhi    !');
    EXPORT Test27 := ASSERT(Uni.ExcludeNthWord('List: abc, def, ghi,   jhi    ',4)+'!' = 'List: abc, def, jhi    !');
    //Check action on a string containing an apostrophe
    EXPORT Test28 := ASSERT(Uni.ExcludeNthWord('I couldn\'t hear you!',4)+'!' = 'I couldn\'t hear !');
    EXPORT Test29 := ASSERT(Uni.ExcludeNthWord('I couldn\'t hear you!',2)+'!' = 'I hear you!!');
    //Check action on a string containing other Symbols
    EXPORT Test30 := ASSERT(Uni.ExcludeNthWord('abc := name',1)+'!' = 'name!');
    EXPORT Test31 := ASSERT(Uni.ExcludeNthWord('abc := name',2)+'!' = 'abc := !');
    //Check action on a string containing different variations/combinations of numbers and other characters
    EXPORT Test32 := ASSERT(Uni.ExcludeNthWord('1 234 123abc 23.6 abc123',1)+'!' = '234 123abc 23.6 abc123!');
    EXPORT Test33 := ASSERT(Uni.ExcludeNthWord('1 234 123abc 23.6 abc123',2)+'!' = '1 123abc 23.6 abc123!');
    EXPORT Test34 := ASSERT(Uni.ExcludeNthWord('1 234 123abc 23.6 abc123',3)+'!' = '1 234 23.6 abc123!');
    EXPORT Test35 := ASSERT(Uni.ExcludeNthWord('1 234 123abc 23.6 abc123',4)+'!' = '1 234 123abc abc123!');
    EXPORT Test36 := ASSERT(Uni.ExcludeNthWord('1 234 123abc 23.6 abc123',5)+'!' = '1 234 123abc 23.6 !');
    //Test other space characters (< 0x20)
    EXPORT Test37 := ASSERT(Uni.ExcludeNthWord('  a b\nc \t  def    ',2)+'!' = '  a c \t  def    !');
    EXPORT Test38 := ASSERT(Uni.ExcludeNthWord('  a b\nc \t  def    ',3)+'!' = '  a b\ndef    !');
    //Check action on a string containing latin diacritical marks
    EXPORT Test39 := ASSERT(Uni.ExcludeNthWord(U'À à',2)+U'!' = U'À !');
    EXPORT Test40 := ASSERT(Uni.ExcludeNthWord(U'ä̰́ Ä̰́',2)+U'!' = U'ä̰́ !');
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls" --> "The deceased the girls" & --> "The deceased changed the"
    EXPORT Test41 := ASSERT(Uni.ExcludeNthWord(U'El difunto cambió las niñas',3)+U'!' = U'El difunto las niñas!');
    EXPORT Test42 := ASSERT(Uni.ExcludeNthWord(U'El difunto cambió las niñas',5)+U'!' = U'El difunto cambió las !');
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer" --> "I am"
    EXPORT Test43 := ASSERT(((integer)Uni.Version() < 50) OR Uni.ExcludeNthWord(U'我是電腦',2)+U'!' = U'我是!'); // Chinese dictionary based iterators added in ICU 50
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?" --> "come here often?"
    EXPORT Test44 := ASSERT(Uni.ExcludeNthWord(U' Έρχεσαι συχνά εδώ; ',1)+U'!' = U'συχνά εδώ; !');
    //Testcases 45 and 46 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning" --> "morning"
    EXPORT Test45 := ASSERT(Uni.ExcludeNthWord(U'صباح الخير',2)+U'!' = U'صباح !');
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test46 := ASSERT(Uni.ExcludeNthWord(U'קָמָץ שִׁי״ן',2)+U'!' = U'קָמָץ !');
   END;
 
 END;
