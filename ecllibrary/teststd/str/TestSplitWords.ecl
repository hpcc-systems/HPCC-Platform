/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestSplitWords := MODULE

  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Str.SplitWords('', '') = global([]));
    EXPORT Test02 := ASSERT(Str.SplitWords('x', '') = global(['x']));
    EXPORT Test03 := ASSERT(Str.SplitWords('x', ' ') = global(['x']));
    EXPORT Test04 := ASSERT(Str.SplitWords(' ', ' ') = global([]));
    EXPORT Test05 := ASSERT(Str.SplitWords('  ', ' ') = global([]));
    EXPORT Test06 := ASSERT(Str.SplitWords('x ', ' ') = global(['x']));
    EXPORT Test07 := ASSERT(Str.SplitWords(' x', ' ') = global(['x']));
    EXPORT Test08 := ASSERT(Str.SplitWords(' x ', ' ') = global(['x']));
    EXPORT Test09 := ASSERT(Str.SplitWords(' abc def ', ' ') = global(['abc','def']));
    EXPORT Test10 := ASSERT(Str.SplitWords(' abc   def ', ' ') = global(['abc','def']));
    EXPORT Test11 := ASSERT(Str.SplitWords(' a b c   def ', ' ') = global(['a','b','c','def']));
    EXPORT Test12 := ASSERT(Str.SplitWords(' abc   def', ' ') = global(['abc','def']));
    EXPORT Test13 := ASSERT(Str.SplitWords('$', '$$') = global(['$']));
    EXPORT Test14 := ASSERT(Str.SplitWords('$x', '$$') = global(['$x']));
    EXPORT Test15 := ASSERT(Str.SplitWords('$$', '$$') = global([]));
    EXPORT Test16 := ASSERT(Str.SplitWords('$$$', '$$') = global(['$']));
    EXPORT Test17 := ASSERT(Str.SplitWords('$$$$', '$$') = global([]));
    EXPORT Test18 := ASSERT(Str.SplitWords('$$x$$', '$$') = global(['x']));
    EXPORT Test19 := ASSERT(Str.SplitWords('$$x$$y', '$$') = global(['x','y']));
    EXPORT Test20 := ASSERT(Str.SplitWords('$$x$$xy', '$$') = global(['x','xy']));
  END;

END;
