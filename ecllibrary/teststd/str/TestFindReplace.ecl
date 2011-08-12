/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestFindReplace := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.FindReplace('','','')+'X' = 'X', CONST);
    EXPORT Test02 := ASSERT(Str.FindReplace('a','','')+'X' = 'aX', CONST);
    EXPORT Test03 := ASSERT(Str.FindReplace('','','')+'X' = 'X', CONST);
    EXPORT Test04 := ASSERT(Str.FindReplace('','','')+'X' = 'X', CONST);
    EXPORT Test05 := ASSERT(Str.FindReplace('abcdef ','','a')+'X' = 'abcdef X', CONST);
    EXPORT Test06 := ASSERT(Str.FindReplace('abcdef ','a','')+'X' = 'bcdef X', CONST);
    EXPORT Test07 := ASSERT(Str.FindReplace('abcdef ','a','$')+'X' = '$bcdef X', CONST);
    EXPORT Test08 := ASSERT(Str.FindReplace('abcdef ','a','$$')+'X' = '$$bcdef X', CONST);
    EXPORT Test09 := ASSERT(Str.FindReplace('abcdef ','abcd','$$')+'X' = '$$ef X', CONST);
    EXPORT Test10 := ASSERT(Str.FindReplace('ababab ','ab','$')+'X' = '$$$ X', CONST);
    EXPORT Test11 := ASSERT(Str.FindReplace('abbabb ','ab','ba')+'X' = 'babbab X', CONST);
    EXPORT Test12 := ASSERT(Str.FindReplace('abababa ','aba','$')+'X' = '$b$ X', CONST);
    EXPORT Test13 := ASSERT(Str.FindReplace('abababa ','a'+x'00'+'ba','$')+'X' = 'abababa X', CONST);
    EXPORT Test14 := ASSERT(Str.FindReplace('aabbaa ','abba','$')+'X' = 'a$a X', CONST);
  END;

END;
