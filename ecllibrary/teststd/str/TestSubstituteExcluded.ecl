/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestSubstituteExcluded := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', 'A', '$')+'X' = '$A$$$$$$$$$$$A$X', CONST);
    EXPORT Test02 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', 'AA', 'Z')+'X' = 'ZAZZZZZZZZZZZAZX', CONST);
    EXPORT Test03 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', 'AA', 'ZZ')+'X' = 'ZAZZZZZZZZZZZAZX', CONST);
    EXPORT Test04 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', 'AA', '')+'X' = ' ABCDEF FEDCBA X', CONST);
    EXPORT Test05 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', 'A ', '$')+'X' = ' A$$$$$ $$$$$A X', CONST);
    EXPORT Test06 := ASSERT(Str.SubstituteExcluded('', 'A ', '$')+'X' = 'X', CONST);
    EXPORT Test07 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', '', '$')+'X' = '$$$$$$$$$$$$$$$X', CONST);
    EXPORT Test08 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', 'a', '$')+'X' = '$$$$$$$$$$$$$$$X', CONST);
    EXPORT Test09 := ASSERT(Str.SubstituteExcluded(' ABCDEF FEDCBA ', ' ABCDEF', '$')+'X' = ' ABCDEF FEDCBA X', CONST);
  END;

END;
