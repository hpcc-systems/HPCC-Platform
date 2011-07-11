/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestCleanSpaces := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT('X'+Str.CleanSpaces('')+'X' = 'XX', CONST);
    EXPORT Test02 := ASSERT('X'+Str.CleanSpaces(' ')+'X' = 'XX', CONST);
    EXPORT Test03 := ASSERT('X'+Str.CleanSpaces('  ')+'X' = 'XX', CONST);
    EXPORT Test04 := ASSERT('X'+Str.CleanSpaces('   ')+'X' = 'XX', CONST);
    EXPORT Test05 := ASSERT('X'+Str.CleanSpaces('  \t  ')+'X' = 'XX', CONST);
    EXPORT Test06 := ASSERT('X'+Str.CleanSpaces('  \t  ')+'X' = 'XX', CONST);
    EXPORT Test07 := ASSERT('X'+Str.CleanSpaces('a b c d')+'X' = 'Xa b c dX', CONST);
    EXPORT Test08 := ASSERT('X'+Str.CleanSpaces(' a b c d ')+'X' = 'Xa b c dX', CONST);
    EXPORT Test09 := ASSERT('X'+Str.CleanSpaces('  a  b  c  d  ')+'X' = 'Xa b c dX', CONST);
    EXPORT Test10 := ASSERT('X'+Str.CleanSpaces('  a \t b\t\tc  d  ')+'X' = 'Xa b c dX', CONST);
    EXPORT Test11 := ASSERT('X'+Str.CleanSpaces('\ta \t b\t\tc  d\t')+'X' = 'Xa b c dX', CONST);
    EXPORT Test12 := ASSERT('X'+Str.CleanSpaces('a\tb\tc\td')+'X' = 'Xa b c dX', CONST);
  END;

END;
