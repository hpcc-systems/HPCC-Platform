IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
value_t := Types.value_t;
dimension_t := Types.dimension_t;
matrix_t := Types.matrix_t;

matrix_t init1 := [1.0, 2.0, -3.0, 4.0, 5.0, -6.0, 7.0, 8.0, 9.0];
value_t half(value_t v, dimension_t r, dimension_t c) := FUNCTION
 RETURN v/2;
END;
value_t zero_d(value_t v, dimension_t r, dimension_t c) := FUNCTION
  RETURN IF(r=c, 0, v);
END;

Test1_mat := BLAS.Apply2Cells(3, 3, init1, half);
Test2_mat := BLAS.Apply2Cells(3, 3, init1, zero_d);

EXPORT Test_Apply2Cells := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=22.5);
    EXPORT Test02 := ASSERT(BLAS.dasum(9, Test2_mat, 1)=30);
  END;
END;
