/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestWordCount := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.WordCount('') = 0, CONST);
    EXPORT Test02 := ASSERT(Str.WordCount('x') = 1);
    EXPORT Test03 := ASSERT(Str.WordCount(' x') = 1);
    EXPORT Test04 := ASSERT(Str.WordCount(' ') = 0);
    EXPORT Test05 := ASSERT(Str.WordCount('  ') = 0);
    EXPORT Test06 := ASSERT(Str.WordCount('x ') = 1);
    EXPORT Test07 := ASSERT(Str.WordCount(' x') = 1);
    EXPORT Test08 := ASSERT(Str.WordCount(' x ') = 1);
    EXPORT Test09 := ASSERT(Str.WordCount(' abc def ') = 2);
    EXPORT Test10 := ASSERT(Str.WordCount(' abc   def ') = 2);
    EXPORT Test11 := ASSERT(Str.WordCount(' a b c   def ') = 4);
    EXPORT Test12 := ASSERT(Str.WordCount(' abc   def') = 2);
    EXPORT Test13 := ASSERT(Str.WordCount(' ,,,, ') = 1);
  END;

END;
