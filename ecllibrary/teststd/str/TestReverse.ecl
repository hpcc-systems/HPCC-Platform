/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestReverse := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.Reverse('')+'X' = 'X', CONST);
    EXPORT Test02 := ASSERT(Str.Reverse('A')+'X' = 'AX', CONST);
    EXPORT Test03 := ASSERT(Str.Reverse('AB')+'X' = 'BAX', CONST);
    EXPORT Test04 := ASSERT(Str.Reverse('ABC')+'X' = 'CBAX', CONST);
    EXPORT Test05 := ASSERT(Str.Reverse('012345678901234567890123456789')+'X' = 
                            '987654321098765432109876543210X', CONST);
  END;

END;
