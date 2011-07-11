/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestExtract := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.Extract('a,b,c,d',1)+'X' = 'aX', CONST);
    EXPORT Test02 := ASSERT(Str.Extract('a,b,c,d',0)+'X' = 'X', CONST);
    EXPORT Test03 := ASSERT(Str.Extract('a,b,c,d',4)+'X' = 'dX', CONST);
    EXPORT Test04 := ASSERT(Str.Extract('a,b,c,d',5)+'X' = 'X', CONST);
    EXPORT Test05 := ASSERT(Str.Extract('a,b,c,d',999999)+'X' = 'X', CONST);
    EXPORT Test06 := ASSERT(Str.Extract(' a , b , c , d ',0)+'X' = 'X', CONST);
    EXPORT Test07 := ASSERT(Str.Extract(' a , b , c , d ',1)+'X' = ' a X', CONST);
    EXPORT Test08 := ASSERT(Str.Extract(' a , b , c , d ',4)+'X' = ' d X', CONST);
    EXPORT Test09 := ASSERT(Str.Extract(' a , b , c , d ',5)+'X' = 'X', CONST);
    EXPORT Test10 := ASSERT(Str.Extract(' a ,, c , d ',2)+'X' = 'X', CONST);
    EXPORT Test11 := ASSERT(Str.Extract(' a ,\', c \', d ',2)+'X' = '\'X', CONST);
    EXPORT Test12 := ASSERT(Str.Extract(' a ,", c ", d ',2)+'X' = '"X', CONST);
    EXPORT Test13 := ASSERT(Str.Extract('',1)+'X' = 'X', CONST);
    EXPORT Test14 := ASSERT(Str.Extract('',0)+'X' = 'X', CONST);
    EXPORT Test15 := ASSERT(Str.Extract('x',-1)+'X' = 'X', CONST);
    EXPORT Test16 := ASSERT(Str.Extract('aaaa,bbbb,cccc,dddd',3)+'X' = 'ccccX', CONST);
  END;

END;
