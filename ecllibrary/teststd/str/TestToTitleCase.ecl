/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestToTitleCase := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.ToTitleCase('@ABCXYZ['+x'60'+'abcdxyz{ '+x'99')+'X' = '@Abcxyz['+x'60'+'Abcdxyz{ '+x'99'+'X', CONST);
    EXPORT Test02 := ASSERT(Str.ToTitleCase('')+'X' = 'X', CONST);
    EXPORT Test03 := ASSERT(Str.ToTitleCase(' John Doe ')+'X' = ' John Doe X', CONST);
    EXPORT Test04 := ASSERT(Str.ToTitleCase(' john doe ')+'X' = ' John Doe X', CONST);
    EXPORT Test05 := ASSERT(Str.ToTitleCase(' john,doe ')+'X' = ' John,Doe X', CONST);
    EXPORT Test06 := ASSERT(Str.ToTitleCase(' JOHN,DOE ')+'X' = ' John,Doe X', CONST);
    EXPORT Test07 := ASSERT(Str.ToTitleCase(' john-doe ')+'X' = ' John-Doe X', CONST);
    EXPORT Test08 := ASSERT(Str.ToTitleCase(' john\tdoe ')+'X' = ' John\tDoe X', CONST);
    EXPORT Test09 := ASSERT(Str.ToTitleCase('john doe')+'X' = 'John DoeX', CONST);
    EXPORT Test10 := ASSERT(Str.ToTitleCase('a b c d')+'X' = 'A B C DX', CONST);
    EXPORT Test11 := ASSERT(Str.ToTitleCase('john macdonald')+'X' = 'John MacdonaldX', CONST);
    EXPORT Test12 := ASSERT(Str.ToTitleCase('99john 5doe')+'X' = '99john 5doeX', CONST);          // could argue j,d should be Titleized
    //This needs to be properly defined.  Should upper/Title work on accented characters if possible
    //EXPORT Test13 := ASSERT(Str.ToTitleCase('jähn mäcdonald')+'X' = 'Jähn MäcdonaldX', CONST);
    //EXPORT Test14 := ASSERT(Str.ToTitleCase('ähn mäcdonald')+'X' = 'Ähn MäcdonaldX', CONST);
  END;

END;
