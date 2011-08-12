/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestWildMatch := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'AB*MN', FALSE), CONST);
    EXPORT Test02 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'A?C*MN', FALSE), CONST);
    EXPORT Test03 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'A*B*C*MN', FALSE), CONST);
    EXPORT Test04 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'A?C?E?G?I?K?M?*', FALSE), CONST);
    EXPORT Test05 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN ', 'A?C?E?G?I?K?M??', FALSE), CONST);
    EXPORT Test06 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMN', 'A?C?E?G?I?K?M??', FALSE), CONST);
    EXPORT Test07 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'A?*?N', FALSE), CONST);
    EXPORT Test08 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'A?*N', FALSE), CONST);
    EXPORT Test09 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'A*N', FALSE), CONST);
    EXPORT Test10 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', '??????????????', FALSE), CONST);
    EXPORT Test11 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', '?BCDEFGHIJKLM?', FALSE), CONST);
    EXPORT Test12 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', '?*N', FALSE), CONST);
    EXPORT Test13 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', '*MN', FALSE), CONST);
    EXPORT Test14 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'ABC*', FALSE), CONST);
    EXPORT Test15 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', '*', FALSE), CONST);
    EXPORT Test16 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMN', '', FALSE), CONST);
    EXPORT Test17 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMN ', 'ABC**MN', FALSE), CONST);
    EXPORT Test18 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'ABC*?*MN', FALSE), CONST);
    EXPORT Test19 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'ABCDEFGHIJKLMN', FALSE), CONST);
    EXPORT Test19a:= ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMN', 'abcdefghijklmn', FALSE), CONST);
    EXPORT Test19b:= ASSERT(Str.WildMatch('ABCDEFGHIJKLMN', 'abcdefghijklmn', TRUE), CONST);
    EXPORT Test20 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMN', 'ABCDEFGHIJKLM', FALSE), CONST);
    EXPORT Test21 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMN', 'BCDEFGHIJKLMN', FALSE), CONST);
    EXPORT Test22 := ASSERT(Str.WildMatch('A*C', 'A*C', FALSE), CONST);
    EXPORT Test23 := ASSERT(Str.WildMatch('A*C', 'A?C', FALSE), CONST);
    EXPORT Test24 := ASSERT(Str.WildMatch('A?C', 'A*C', FALSE), CONST);
    EXPORT Test25 := ASSERT(Str.WildMatch('A?C', 'A?C', FALSE), CONST);
    EXPORT Test27 := ASSERT(Str.WildMatch('', '', FALSE), CONST);
    EXPORT Test28 := ASSERT(Str.WildMatch('', '*', FALSE), CONST);
    EXPORT Test29 := ASSERT(NOT Str.WildMatch('', '?', FALSE), CONST);
    EXPORT Test30 := ASSERT(NOT Str.WildMatch('', 'A', FALSE), CONST);
    EXPORT Test31 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 'A***********z', FALSE));
    EXPORT Test32 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 'A***********Z', FALSE));
    EXPORT Test33 := ASSERT(Str.WildMatch('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', '*opqrstuvwxyz', FALSE));
    EXPORT Test34 := ASSERT(NOT Str.WildMatch('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', '*xpqrstuvwxyz', FALSE));
  END;

END;
