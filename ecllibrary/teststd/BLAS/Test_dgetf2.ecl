IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
Types.matrix_t init_lower := [1, 2, 3, 0, 1, 4, 0, 0, 1];
Types.matrix_t init_upper := [2, 0, 0, 3, 4, 0, 9, 16, 25];

input := BLAS.dgemm(FALSE, FALSE, 3, 3, 3, 1.0, init_lower, init_upper);
Test1_mat := BLAS.dgetf2(3, 3, input);

EXPORT Test_dgetf2 := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=68);
  END;
END;
