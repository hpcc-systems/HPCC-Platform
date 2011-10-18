/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestCountWords := MODULE

  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Str.CountWords('', '') = 0);
    EXPORT Test02 := ASSERT(Str.CountWords('x', '') = 1);
    EXPORT Test03 := ASSERT(Str.CountWords('x', ' ') = 1);
    EXPORT Test04 := ASSERT(Str.CountWords(' ', ' ') = 0);
    EXPORT Test05 := ASSERT(Str.CountWords('  ', ' ') = 0);
    EXPORT Test06 := ASSERT(Str.CountWords('x ', ' ') = 1);
    EXPORT Test07 := ASSERT(Str.CountWords(' x', ' ') = 1);
    EXPORT Test08 := ASSERT(Str.CountWords(' x ', ' ') = 1);
    EXPORT Test09 := ASSERT(Str.CountWords(' abc def ', ' ') = 2);
    EXPORT Test10 := ASSERT(Str.CountWords(' abc   def ', ' ') = 2);
    EXPORT Test11 := ASSERT(Str.CountWords(' a b c   def ', ' ') = 4);
    EXPORT Test12 := ASSERT(Str.CountWords(' abc   def', ' ') = 2);
    EXPORT Test13 := ASSERT(Str.CountWords('$', '$$') = 1);
    EXPORT Test14 := ASSERT(Str.CountWords('$x', '$$') = 1);
    EXPORT Test15 := ASSERT(Str.CountWords('$$', '$$') = 0);
    EXPORT Test16 := ASSERT(Str.CountWords('$$$', '$$') = 1);
    EXPORT Test17 := ASSERT(Str.CountWords('$$$$', '$$') = 0);
    EXPORT Test18 := ASSERT(Str.CountWords('$$x$$', '$$') = 1);
    EXPORT Test19 := ASSERT(Str.CountWords('$$x$$y', '$$') = 2);
    EXPORT Test20 := ASSERT(Str.CountWords('$$x$$xy', '$$') = 2);
    EXPORT Test21 := ASSERT(Str.CountWords('a,c,d', ',', TRUE) = 3);
    EXPORT Test22 := ASSERT(Str.CountWords('a,,d', ',', TRUE) = 3);
    EXPORT Test23 := ASSERT(Str.CountWords(',,,', ',', TRUE) = 4);
  END;

END;
