/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestToLowerCase := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.ToLowerCase('@ABCXYZ['+x'60'+'abcdxyz{ '+x'99')+'X' = '@abcxyz['+x'60'+'abcdxyz{ '+x'99'+'X', CONST);
    EXPORT Test02 := ASSERT(Str.ToLowerCase('')+'X' = 'X', CONST);
  END;

END;
