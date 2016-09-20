IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
Types.matrix_t initC := [1, 1, 1, 2, 2, 2, 3, 3, 3];
Types.matrix_t initA := [1, 1, 1];
Types.matrix_t mat_A := [1, 2, 2, 3, 3, 4];

Test1_mat := BLAS.dsyrk(Types.Triangle.upper, FALSE, 3, 1, 1, initA, 1, initC, TRUE);
Test2_mat := BLAS.dsyrk(Types.Triangle.lower, TRUE, 3, 1, 1, initA, 1, initC, TRUE);
Test3_mat := BLAS.dsyrk(Types.Triangle.Upper, FALSE, 3, 2, 1, mat_a, 1, Test1_mat, FALSE);
Test4_mat := BLAS.dsyrk(Types.Triangle.Lower, TRUE, 3, 2, 1, mat_a, 1, Test2_mat, FALSE);

EXPORT Test_dsyrk := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=20);
    EXPORT Test02 := ASSERT(BLAS.dasum(9, Test2_mat, 1)=16);
    EXPORT Test03 := ASSERT(BLAS.dasum(9, Test3_mat, 1)=104);
    EXPORT Test04 := ASSERT(BLAS.dasum(9, Test4_mat, 1)=96);
  END;
END;
