/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestFromHexPairs := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.FromHexPairs('')+D'X' = D'X', CONST);
    EXPORT Test02 := ASSERT(Str.FromHexPairs('30313233')+D'X' = D'0123X', CONST);
    EXPORT Test03 := ASSERT(Str.FromHexPairs('0001FF80')+D'X' = D'\000\001\377\200X', CONST);
    EXPORT Test04 := ASSERT(Str.FromHexPairs('   00  01  FF 80   ')+D'X' = D'\000\001\377\200X', CONST);
    EXPORT Test05 := ASSERT(Str.FromHexPairs('   XX  X1  FF 80   ')+D'X' = D'\000\001\377\200X', CONST);
    EXPORT Test06 := ASSERT(Str.FromHexPairs('   XX  X1  ff 80   ')+D'X' = D'\000\001\377\200X', CONST);
    EXPORT Test07 := ASSERT(Str.FromHexPairs('   XX  X1  ff 80   ')+D'X' = D'\000\001\377\200X', CONST);
    EXPORT Test08 := ASSERT(Str.FromHexPairs('   @g  `1  ff 80   ')+D'X' = D'\000\001\377\200X', CONST);
    EXPORT Test09 := ASSERT(Str.FromHexPairs('   @g  `1  ff 80 2')+D'X' = D'\000\001\377\200X', CONST);
//    EXPORT Test10 := ASSERT(Str.FromHexPairs('   @g  `1  ff 80 2 ')+D'X' = D'\000\001\377\200X', CONST);  // undefined!
  END;

END;
