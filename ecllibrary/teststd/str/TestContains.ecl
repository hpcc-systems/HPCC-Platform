/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestContains := MODULE

  EXPORT TestConstant1 := MODULE
    EXPORT Test01 := ASSERT(Str.Contains('ABCDEF ABCDEF', 'A', FALSE), CONST);
    EXPORT Test02 := ASSERT(Str.Contains('ABCDEF ABCDEF', 'AA', FALSE), CONST);
    EXPORT Test03 := ASSERT(Str.Contains('ABCDEF ABCDEF', 'ABCDEF', FALSE), CONST);
    EXPORT Test04 := ASSERT(NOT Str.Contains('ABCDEF ABCDEF', 'AAA', FALSE), CONST);
    EXPORT Test05 := ASSERT(Str.Contains('ABCDEF ABCDEF', 'FEDC', FALSE), CONST);
    EXPORT Test06 := ASSERT(Str.Contains('abcdef abcdef', 'A', TRUE), CONST);
    EXPORT Test07 := ASSERT(Str.Contains('ABCDEF ABCDEF', 'aa', TRUE), CONST);
    EXPORT Test08 := ASSERT(NOT Str.Contains('abcdef abcdef', 'A', FALSE), CONST);
    EXPORT Test09 := ASSERT(Str.Contains('', '', FALSE), CONST);
    EXPORT Test10 := ASSERT(Str.Contains('x', '', FALSE), CONST);
    EXPORT Test11 := ASSERT(Str.Contains(' ', ' ', FALSE), CONST);
    EXPORT Test12 := ASSERT(NOT Str.Contains(' ', '\377', FALSE), CONST);
    EXPORT Test13 := ASSERT(Str.Contains('\377', '\377', FALSE), CONST);
    EXPORT Test14 := ASSERT(NOT Str.Contains('abcdef', '\367\370\350\354\360\364', FALSE), CONST);
    EXPORT Test15 := ASSERT(NOT Str.Contains('abcdef', '\256\267\377', FALSE), CONST);
  END;

END;
