/*##############################################################################

## Copyright � 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestToCapitalCase := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.ToCapitalCase('@ABCXYZ['+x'60'+'abcdxyz{ '+x'99')+'X' = '@ABCXYZ['+x'60'+'Abcdxyz{ '+x'99'+'X', CONST);
    EXPORT Test02 := ASSERT(Str.ToCapitalCase('')+'X' = 'X', CONST);
    EXPORT Test03 := ASSERT(Str.ToCapitalCase(' John Doe ')+'X' = ' John Doe X', CONST);
    EXPORT Test04 := ASSERT(Str.ToCapitalCase(' john doe ')+'X' = ' John Doe X', CONST);
    EXPORT Test05 := ASSERT(Str.ToCapitalCase(' john,doe ')+'X' = ' John,Doe X', CONST);
    EXPORT Test06 := ASSERT(Str.ToCapitalCase(' JOHN,DOE ')+'X' = ' JOHN,DOE X', CONST);
    EXPORT Test07 := ASSERT(Str.ToCapitalCase(' john-doe ')+'X' = ' John-Doe X', CONST);
    EXPORT Test08 := ASSERT(Str.ToCapitalCase(' john\tdoe ')+'X' = ' John\tDoe X', CONST);
    EXPORT Test09 := ASSERT(Str.ToCapitalCase('john doe')+'X' = 'John DoeX', CONST);
    EXPORT Test10 := ASSERT(Str.ToCapitalCase('a b c d')+'X' = 'A B C DX', CONST);
    EXPORT Test11 := ASSERT(Str.ToCapitalCase('john macdonald')+'X' = 'John MacdonaldX', CONST);
    EXPORT Test12 := ASSERT(Str.ToCapitalCase('99john 5doe')+'X' = '99john 5doeX', CONST);          // could argue j,d should be capitalized
    //This needs to be properly defined.  Should upper/capital work on accented characters if possible
    //EXPORT Test13 := ASSERT(Str.ToCapitalCase('j채hn m채cdonald')+'X' = 'J채hn M채cdonaldX', CONST);
    //EXPORT Test14 := ASSERT(Str.ToCapitalCase('채hn m채cdonald')+'X' = '횆hn M채cdonaldX', CONST);
  END;

END;
