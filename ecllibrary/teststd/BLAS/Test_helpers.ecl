IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types;

Types.matrix_t x := [1.0, 2.0, 3.0, 2.0, 2.0, 2.0, 4.0, 4.0, 4.0];
Types.matrix_t init1 := [1.0, 2.0, 3.0, 4.0];

Test_d0 := BLAS.extract_diag(3, 3, x);
Test_norm1 := BLAS.dasum(3, Test_d0, 1);
Test_trace := BLAS.trace(3, 3, x);
Test_Diag1 := BLAS.make_diag(4, -1.0);
Test_Diag2 := BLAS.make_diag(4, 7.0, init1);
Test_vector := BLAS.make_vector(5, 4);

EXPORT Test_helpers := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Test_norm1=7);
    EXPORT Test02 := ASSERT(Test_trace=7);
    EXPORT Test03 := ASSERT(BLAS.dasum(16, Test_Diag1, 1)=4);
    EXPORT Test04 := ASSERT(BLAS.dasum(16, Test_Diag2, 1)=70);
    EXPORT Test05 := ASSERT(BLAS.dasum(5, Test_vector, 1)=20);
  END;
END;