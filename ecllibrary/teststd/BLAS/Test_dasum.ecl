IMPORT Std.BLAS AS BLAS;
SET OF REAL8 init1 := [1.0, 2.0, -3.0, 4.0, 5.0, -6.0, 7.0, 8.0, 9.0];

EXPORT Test_dasum := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(4, init1, 1)=10);
    EXPORT Test02 := ASSERT(BLAS.dasum(4, init1, 2)=16);
    EXPORT Test03 := ASSERT(BLAS.dasum(3, init1, 3, 2)=18);
    EXPORT Test04 := ASSERT(BLAS.dasum(3, init1, 1, 3)=15);
  END;
END;
