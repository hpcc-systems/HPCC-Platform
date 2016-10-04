IMPORT Std.BLAS AS BLAS;
IMPORT Std.BLAS.Types as Types;
Types.matrix_t init1 := [1, 1, 1, 2, 2, 2, 3, 3, 3];
Test1_mat := BLAS.dscal(9, 2.0, init1, 1); //all 9 as 1 vector
Test2_mat := BLAS.dscal(3, 2.0, init1, 3);  // row 2 as 3x3
Test3_mat := BLAS.dscal(3, 2.0, init1, 1, 3); // column 2
EXPORT Test_dscal := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=36);
    EXPORT Test02 := ASSERT(BLAS.dasum(9, Test2_mat, 1)=24);
    EXPORT Test03 := ASSERT(BLAS.dasum(9, Test3_mat, 1)=24);
  END;
END;
