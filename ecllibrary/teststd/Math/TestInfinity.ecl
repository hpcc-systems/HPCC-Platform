/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Math;

dbz := #option('divideByZero', 'nan');

EXPORT TestInfinity := MODULE

  EXPORT TestConstant := MODULE
    EXPORT TestInf1 := WHEN(ASSERT(Math.IsInfinite(Math.Infinity), CONST), dbz);
    EXPORT TestInf2 := ASSERT(Math.IsInfinite(-Math.Infinity), CONST);
    EXPORT TestInf3 := ASSERT(NOT Math.IsNan(Math.Infinity), CONST);
    EXPORT TestInf4 := ASSERT(NOT Math.IsFinite(Math.Infinity), CONST);

    EXPORT TestNan1 := ASSERT(NOT Math.IsInfinite(Math.NaN), CONST);
    EXPORT TestNan2 := ASSERT(Math.IsNan(Math.NaN), CONST);
    EXPORT TestNan3 := ASSERT(NOT Math.IsFinite(Math.NaN), CONST);

    EXPORT TestNorm1 := ASSERT(NOT Math.IsInfinite(0.0), CONST);
    EXPORT TestNorm2 := ASSERT(NOT Math.IsNan(0.0), CONST);
    EXPORT TestNorm3 := ASSERT(Math.IsFinite(0.0), CONST);
  END;

  EXPORT TestVariable := MODULE
    SHARED zero := nofold(0.0);
    EXPORT TestInf1 := ASSERT(Math.IsInfinite(1.0/zero));
    EXPORT TestInf2 := ASSERT(Math.IsInfinite(log(zero)));
    EXPORT TestInf3 := ASSERT(NOT Math.IsNan(2.0/zero));
    EXPORT TestInf4 := ASSERT(NOT Math.IsFinite(3.0/zero));

    EXPORT TestNan1 := ASSERT(NOT Math.IsInfinite(log(zero - 1.0)));
    EXPORT TestNan2 := ASSERT(Math.IsNan(log(zero - 2.0)));
    EXPORT TestNan3 := ASSERT(NOT Math.IsFinite(log(zero - 3.0)));

    EXPORT TestNorm1 := ASSERT(NOT Math.IsInfinite(zero));
    EXPORT TestNorm2 := ASSERT(NOT Math.IsNan(zero));
    EXPORT TestNorm3 := ASSERT(Math.IsFinite(zero));
  END;

END;
