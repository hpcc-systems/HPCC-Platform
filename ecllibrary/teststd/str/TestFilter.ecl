/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestFilter := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.Filter('ABCDEFFEDCBA', 'ABC')+'X' = 'ABCCBAX', CONST);
    EXPORT Test02 := ASSERT(Str.Filter('ABCDEFFEDCBA', '')+'X' = 'X', CONST);
    EXPORT Test03 := ASSERT(Str.Filter('ABCDEFFEDCBA', 'ABCDEF')+'X' = 'ABCDEFFEDCBAX', CONST);
    EXPORT Test04 := ASSERT(Str.Filter('ABCDEFFEDCBA', 'FEDCBA')+'X' = 'ABCDEFFEDCBAX', CONST);
    EXPORT Test05 := ASSERT(Str.Filter('ABCDEFFEDCBA', x'00'+'CCCCC')+'X' = 'CCX', CONST);
    EXPORT Test06 := ASSERT(Str.Filter(x'00'+'ABCDEF' + x'00' + 'FEDCBA', x'00'+'CCCCC')+'X' = x'00'+'C'+x'00'+'CX', CONST);
    EXPORT Test07 := ASSERT(Str.Filter(' ABCDEF FEDCBA ', 'BCDE')+'X' = 'BCDEEDCBX', CONST);
    EXPORT Test08 := ASSERT(Str.Filter(' ABCDEF FEDCBA ', 'BCDE ')+'X' = ' BCDE EDCB X', CONST);
    EXPORT Test09 := ASSERT(Str.Filter(' ABCDEF FEDCBA ', x'FF'+'BCDE ')+'X' = ' BCDE EDCB X', CONST);
    EXPORT Test10 := ASSERT(Str.Filter('ABCDEFFEDCBA', 'A')+'X' = 'AAX', CONST);
  END;

END;
