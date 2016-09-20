IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
Types.matrix_t init1 := [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
Types.matrix_t init2 := [9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];

Test1_mat := BLAS.daxpy(9, 1.0, init1, 1, init2, 1);
Test2_mat := BLAS.daxpy(3, -1.0, init1, 3, init2, 3, 2, 2);

EXPORT Test_daxpy := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=90);
    EXPORT Test02 := ASSERT(BLAS.dasum(9, Test2_mat, 1)=47);
  END;
END;
