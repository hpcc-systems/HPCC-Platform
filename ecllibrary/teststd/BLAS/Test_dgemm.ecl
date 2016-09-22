IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
Types.matrix_t init1 := [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
Types.matrix_t init2 := [9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
Types.matrix_t init3 := [1.0, 2.0, 3.0];
Types.matrix_t init4 := [2.0, 2.0, 2.0];

Test1_mat := BLAS.dgemm(FALSE, TRUE, 3, 3, 1, 1.0, init3, init4);
Test2_mat := BLAS.dgemm(TRUE, FALSE, 1, 1, 3, 1.0, init3, init4);
Test3_mat := BLAS.dgemm(FALSE, TRUE, 3, 1, 3, -1.0, init1, init4);
Test4_mat := BLAS.dgemm(FALSE, TRUE, 3, 1, 3, 1.0, init1, init4, 5, init3);

EXPORT Test_dgemm := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=36);
    EXPORT Test02 := ASSERT(BLAS.dasum(1, Test2_mat, 1)=12);
    EXPORT Test03 := ASSERT(BLAS.dasum(3, Test3_mat, 1)=90);
    EXPORT Test04 := ASSERT(BLAS.dasum(3, Test4_mat, 1)=120);
  END;
END;
