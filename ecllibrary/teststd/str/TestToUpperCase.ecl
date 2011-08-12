/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestToUpperCase := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.ToUpperCase('@ABCXYZ['+x'60'+'abcdxyz{ '+x'99')+'X' = '@ABCXYZ['+x'60'+'ABCDXYZ{ '+x'99'+'X', CONST);
    EXPORT Test02 := ASSERT(Str.ToUpperCase('')+'X' = 'X', CONST);
  END;

END;
