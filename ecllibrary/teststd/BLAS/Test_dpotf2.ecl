IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
Types.matrix_t init1 := [4, 6, 8, 6, 13, 18, 8, 18, 29];
Types.matrix_t init2 := [4, 0, 0, 0, 9, 0, 0, 0, 16];

Test1_mat := BLAS.dpotf2(Types.Triangle.lower, 3, init1);
Test2_mat := BLAS.dpotf2(Types.Triangle.upper, 3, init1, FALSE);
Test3_mat := BLAS.dpotf2(Types.Triangle.upper, 3, init2);

EXPORT Test_dpotf2 := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=16);
    EXPORT Test02 := ASSERT(BLAS.dasum(9, Test2_mat, 1)=48);
    EXPORT Test03 := ASSERT(BLAS.dasum(9, Test3_mat, 1)=9);
  END;
END;
