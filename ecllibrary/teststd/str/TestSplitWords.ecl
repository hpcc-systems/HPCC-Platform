/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestSplitWords := MODULE

  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Str.SplitWords('', '') = []);
    EXPORT Test02 := ASSERT(Str.SplitWords('x', '') = ['x']);
    EXPORT Test03 := ASSERT(Str.SplitWords('x', ' ') = ['x']);
    EXPORT Test04 := ASSERT(Str.SplitWords(' ', ' ') = []);
    EXPORT Test05 := ASSERT(Str.SplitWords('  ', ' ') = []);
    EXPORT Test06 := ASSERT(Str.SplitWords('x ', ' ') = ['x']);
    EXPORT Test07 := ASSERT(Str.SplitWords(' x', ' ') = ['x']);
    EXPORT Test08 := ASSERT(Str.SplitWords(' x ', ' ') = ['x']);
    EXPORT Test09 := ASSERT(Str.SplitWords(' abc def ', ' ') = ['abc','def']);
    EXPORT Test10 := ASSERT(Str.SplitWords(' abc   def ', ' ') = ['abc','def']);
    EXPORT Test11 := ASSERT(Str.SplitWords(' a b c   def ', ' ') = ['a','b','c','def']);
    EXPORT Test12 := ASSERT(Str.SplitWords(' abc   def', ' ') = ['abc','def']);
    EXPORT Test13 := ASSERT(Str.SplitWords('$', '$$') = ['$']);
    EXPORT Test14 := ASSERT(Str.SplitWords('$x', '$$') = ['$x']);
    EXPORT Test15 := ASSERT(Str.SplitWords('$$', '$$') = []);
    EXPORT Test16 := ASSERT(Str.SplitWords('$$$', '$$') = ['$']);
    EXPORT Test17 := ASSERT(Str.SplitWords('$$$$', '$$') = []);
    EXPORT Test18 := ASSERT(Str.SplitWords('$$x$$', '$$') = ['x']);
    EXPORT Test19 := ASSERT(Str.SplitWords('$$x$$y', '$$') = ['x','y']);
    EXPORT Test20 := ASSERT(Str.SplitWords('$$x$$xy', '$$') = ['x','xy']);
    EXPORT Test21 := ASSERT(Str.SplitWords('$$x$$xy', '$$', TRUE) = ['','x','xy']);
    EXPORT Test22 := ASSERT(Str.SplitWords('$$$$', '$$',TRUE) = ['','','']);
  END;

END;
