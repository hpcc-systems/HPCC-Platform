/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestFilterOut := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.FilterOut('ABCDEFFEDCBA', 'ABC')+'X' = 'DEFFEDX', CONST);
    EXPORT Test02 := ASSERT(Str.FilterOut('ABCDEFFEDCBA', '')+'X' = 'ABCDEFFEDCBAX', CONST);
    EXPORT Test03 := ASSERT(Str.FilterOut('ABCDEFFEDCBA', 'ABCDEF')+'X' = 'X', CONST);
    EXPORT Test04 := ASSERT(Str.FilterOut('ABCDEFFEDCBA', 'FEDCBA')+'X' = 'X', CONST);
    EXPORT Test05 := ASSERT(Str.FilterOut('ABCDEFFEDCBA', x'00'+'CCCCC')+'X' = 'ABDEFFEDBAX', CONST);
    EXPORT Test06 := ASSERT(Str.FilterOut(x'00'+'ABCDEF' + x'00' + 'FEDCBA', x'00'+'CCCCC')+'X' = 'ABDEFFEDBAX', CONST);
    EXPORT Test07 := ASSERT(Str.FilterOut(' ABCDEF FEDCBA ', 'BCDE')+'X' = ' AF FA X', CONST);
    EXPORT Test08 := ASSERT(Str.FilterOut(' ABCDEF FEDCBA ', 'BCDE ')+'X' = 'AFFAX', CONST);
    EXPORT Test09 := ASSERT(Str.FilterOut(' ABCDEF FEDCBA ', x'FF'+'BCDE ')+'X' = 'AFFAX', CONST);
    EXPORT Test10 := ASSERT(Str.FilterOut('ABCDEFFEDCBA', 'A')+'X' = 'BCDEFFEDCBX', CONST);
  END;

END;
