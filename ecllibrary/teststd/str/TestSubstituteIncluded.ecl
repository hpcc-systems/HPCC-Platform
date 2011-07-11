/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestSubstituteIncluded := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'A', 'Z')+'X' = ' ZBCDEF FEDCBZ X', CONST);
    EXPORT Test01a := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'A', 'ZZ')+'X' = ' ZBCDEF FEDCBZ X', CONST);
    EXPORT Test02 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'AA', 'Z')+'X' = ' ZBCDEF FEDCBZ X', CONST);
    EXPORT Test03 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'AA', 'ZZ')+'X' = ' ZBCDEF FEDCBZ X', CONST);
    EXPORT Test04 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'AA', '')+'X' = ' ABCDEF FEDCBA X', CONST);
    EXPORT Test05 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'A ', '$')+'X' = '$$BCDEF$FEDCB$$X', CONST);
    EXPORT Test06 := ASSERT(Str.SubstituteIncluded('', 'A ', '$')+'X' = 'X', CONST);
    EXPORT Test07 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', '', '$')+'X' = ' ABCDEF FEDCBA X', CONST);
    EXPORT Test08 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', 'a', '$')+'X' = ' ABCDEF FEDCBA X', CONST);
    EXPORT Test09 := ASSERT(Str.SubstituteIncluded(' ABCDEF FEDCBA ', ' ABCDEF', '$')+'X' = '$$$$$$$$$$$$$$$X', CONST);
  END;

END;
