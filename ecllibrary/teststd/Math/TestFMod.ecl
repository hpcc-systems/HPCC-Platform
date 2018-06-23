/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Math;

dbz := #option('divideByZero', 'nan');

EXPORT TestFMod := MODULE
  SHARED epsilon := 0.000001;

  EXPORT TestConstant := MODULE
    EXPORT TestFMod1 := WHEN(ASSERT(Math.IsNan(Math.FMod(1.0, 0.0)), CONST), dbz);
    EXPORT TestFMod2 := ASSERT(Math.FMatch(Math.FMod(5.3, 2.0), 1.3, epsilon), CONST);
    EXPORT TestFMod3 := ASSERT(Math.FMatch(Math.FMod(18.5, 4.2), 1.7, epsilon), CONST);
  END;

  EXPORT TestVariable := MODULE
    SHARED zero := NOFOLD(0.0);
    EXPORT TestFMod1 := WHEN(ASSERT(Math.IsNan(Math.FMod(1.0, zero))), dbz);
    EXPORT TestFMod2 := ASSERT(Math.FMatch(Math.FMod(5.3, 2.0+zero), 1.3, epsilon));
    EXPORT TestFMod3 := ASSERT(Math.FMatch(Math.FMod(18.5, 4.2+zero), 1.7, epsilon));
  END;

END;
