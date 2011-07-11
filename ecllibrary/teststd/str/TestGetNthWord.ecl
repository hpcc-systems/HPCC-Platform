/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestGetNthWord := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.GetNthWord('',0)+'!' = '!', CONST);
    EXPORT Test02 := ASSERT(Str.GetNthWord('',1)+'!' = '!', CONST);
    EXPORT Test03 := ASSERT(Str.GetNthWord('',-1)+'!' = '!', CONST);
    EXPORT Test04 := ASSERT(Str.GetNthWord('             ',0)+'!' = '!', CONST);
    EXPORT Test05 := ASSERT(Str.GetNthWord('             ',1)+'!' = '!', CONST);
    EXPORT Test06 := ASSERT(Str.GetNthWord('             ',-1)+'!' = '!', CONST);
    EXPORT Test07 := ASSERT(Str.GetNthWord('x',0)+'!' = '!');
    EXPORT Test08 := ASSERT(Str.GetNthWord('x',1)+'!' = 'x!');
    EXPORT Test09 := ASSERT(Str.GetNthWord('x',2)+'!' = '!');
    EXPORT Test10 := ASSERT(Str.GetNthWord('x',3)+'!' = '!');
    EXPORT Test11 := ASSERT(Str.GetNthWord(' x',1)+'!' = 'x!');
    EXPORT Test12 := ASSERT(Str.GetNthWord('x ',1)+'!' = 'x!');
    EXPORT Test13 := ASSERT(Str.GetNthWord(' x',1)+'!' = 'x!');
    EXPORT Test14 := ASSERT(Str.GetNthWord(' x ',1)+'!' = 'x!');
    EXPORT Test15 := ASSERT(Str.GetNthWord(' abc def ', 1)+'!' = 'abc!');
    EXPORT Test16 := ASSERT(Str.GetNthWord(' abc def ', 2)+'!' = 'def!');
    EXPORT Test17 := ASSERT(Str.GetNthWord(' a b c   def ',3)+'!' = 'c!');
    EXPORT Test18 := ASSERT(Str.GetNthWord(' ,,,, ',1)+'!' = ',,,,!');
  END;

END;
