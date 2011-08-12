/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestGetNthWord := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Uni.GetNthWord(U'',0)+U'!' = U'!', CONST);
    EXPORT Test02 := ASSERT(Uni.GetNthWord(U'',1)+U'!' = U'!', CONST);
    EXPORT Test03 := ASSERT(Uni.GetNthWord(U'',-1)+U'!' = U'!', CONST);
    EXPORT Test04 := ASSERT(Uni.GetNthWord(U'             ',0)+U'!' = U'!', CONST);
    EXPORT Test05 := ASSERT(Uni.GetNthWord(U'             ',1)+U'!' = U'!', CONST);
    EXPORT Test06 := ASSERT(Uni.GetNthWord(U'             ',-1)+U'!' = U'!', CONST);
    EXPORT Test07 := ASSERT(Uni.GetNthWord(U'x',0)+U'!' = U'!');
    EXPORT Test08 := ASSERT(Uni.GetNthWord(U'x',1)+U'!' = U'x!');
    EXPORT Test09 := ASSERT(Uni.GetNthWord(U'x',2)+U'!' = U'!');
    EXPORT Test10 := ASSERT(Uni.GetNthWord(U'x',3)+U'!' = U'!');
    EXPORT Test11 := ASSERT(Uni.GetNthWord(U' x',1)+U'!' = U'x!');
    EXPORT Test12 := ASSERT(Uni.GetNthWord(U'x ',1)+U'!' = U'x!');
    EXPORT Test13 := ASSERT(Uni.GetNthWord(U' x',1)+U'!' = U'x!');
    EXPORT Test14 := ASSERT(Uni.GetNthWord(U' x ',1)+U'!' = U'x!');
    EXPORT Test15 := ASSERT(Uni.GetNthWord(U' abc def ', 1)+U'!' = U'abc!');
    EXPORT Test16 := ASSERT(Uni.GetNthWord(U' abc def ', 2)+U'!' = U'def!');
    EXPORT Test17 := ASSERT(Uni.GetNthWord(U' a b c   def ',3)+U'!' = U'c!');
    EXPORT Test18 := ASSERT(Uni.GetNthWord(U' ,,,, ',1)+U'!' = U'!');
  END;

END;
