IMPORT Std.BLAS;
IMPORT BLAS.Types;
Side := Types.Side;
Diagonal := Types.Diagonal;
Triangle := Types.Triangle;
Types.matrix_t left_a0 := [2, 3, 4, 0, 2, 3, 0, 0, 2];
Types.matrix_t right_a0 := [2, 0, 0, 3, 2, 0, 4, 3, 2];
Types.matrix_t composite_a0 := [2, 3, 4, 3, 2, 3, 4, 3, 2];
Types.matrix_t composite_a1 := [4, 1.5, 2, 6, 4, 1.5, 8, 6, 4];
Types.matrix_t mat_b := [4, 6, 8, 6, 13, 18, 8, 18, 29];

Types.matrix_t ident := [1, 0, 0, 0, 1, 0, 0, 0, 1];

Test1_mat := BLAS.dtrsm(Side.Ax, Triangle.Lower, FALSE, Diagonal.NotUnitTri,
                        3, 3, 3, 1.0, left_a0, mat_b);
Test2_mat := BLAS.dtrsm(Side.xA, Triangle.Upper, FALSE, Diagonal.NotUnitTri,
                        3, 3, 3, 1.0, right_a0, mat_b);
Test3_mat := BLAS.dtrsm(Side.Ax, Triangle.Upper, TRUE, Diagonal.NotUnitTri,
                        3, 3, 3, 1.0, right_a0, mat_b);
Test4_mat := BLAS.dtrsm(Side.Ax, Triangle.Lower, FALSE, Diagonal.NotUnitTri,
                        3, 3, 3, 1.0, composite_a0, mat_b);
Test5_mat := BLAS.dtrsm(Side.Ax, Triangle.Lower, FALSE, Diagonal.UnitTri,
                        3, 3, 3, 1.0, composite_a1, mat_b);

EXPORT Test_dtrsm := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(BLAS.dasum(9, Test1_mat, 1)=16);
    EXPORT Test02 := ASSERT(BLAS.dasum(9, Test2_mat, 1)=16);
    EXPORT Test03 := ASSERT(BLAS.dasum(9, Test3_mat, 1)=16);
    EXPORT Test04 := ASSERT(BLAS.dasum(9, Test4_mat, 1)=16);
    EXPORT Test05 := ASSERT(BLAS.dasum(9, Test5_mat, 1)=32);
  END;
END;
