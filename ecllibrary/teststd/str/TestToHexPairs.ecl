/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestToHexPairs := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.ToHexPairs(D'')+'X' = 'X', CONST);
    EXPORT Test02 := ASSERT(Str.ToHexPairs(D'0123')+'X' = '30313233X', CONST);
    EXPORT Test03 := ASSERT(Str.ToHexPairs(D'\000\001\377\200')+'X' = '0001FF80X', CONST);
  END;

END;
