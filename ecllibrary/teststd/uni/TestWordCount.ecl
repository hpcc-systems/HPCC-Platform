/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestWordCount := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Uni.WordCount(U'') = 0, CONST);
    EXPORT Test02 := ASSERT(Uni.WordCount(U'x') = 1);
    EXPORT Test03 := ASSERT(Uni.WordCount(U' x') = 1);
    EXPORT Test04 := ASSERT(Uni.WordCount(U' ') = 0);
    EXPORT Test05 := ASSERT(Uni.WordCount(U'  ') = 0);
    EXPORT Test06 := ASSERT(Uni.WordCount(U'x ') = 1);
    EXPORT Test07 := ASSERT(Uni.WordCount(U' x') = 1);
    EXPORT Test08 := ASSERT(Uni.WordCount(U' x ') = 1);
    EXPORT Test09 := ASSERT(Uni.WordCount(U' abc def ') = 2);
    EXPORT Test10 := ASSERT(Uni.WordCount(U' abc   def ') = 2);
    EXPORT Test11 := ASSERT(Uni.WordCount(U' a b c   def ') = 4);
    EXPORT Test12 := ASSERT(Uni.WordCount(U' abc   def') = 2);
    EXPORT Test13 := ASSERT(Uni.WordCount(U' ,,,, ') = 0);
  END;

END;
