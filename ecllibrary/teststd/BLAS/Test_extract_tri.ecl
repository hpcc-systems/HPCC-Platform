IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types;
Diagonal := Types.Diagonal;
Triangle := Types.Triangle;

SET OF REAL8 init1 := [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];

EXPORT Test_extract_tri := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT test01 := ASSERT(BLAS.extract_tri(3, 3, Triangle.Upper, Diagonal.UnitTri, init1)   =[1.0,0.0,0.0,4.0,1.0,0.0,7.0,8.0,1.0]);
    EXPORT test02 := ASSERT(BLAS.extract_tri(3, 3, Triangle.Upper, Diagonal.NotUnitTri, init1)=[1.0,0.0,0.0,4.0,5.0,0.0,7.0,8.0,9.0]);
    EXPORT test03 := ASSERT(BLAS.extract_tri(3, 3, Triangle.Lower, Diagonal.UnitTri, init1)   =[1.0,2.0,3.0,0.0,1.0,6.0,0.0,0.0,1.0]);
    EXPORT test04 := ASSERT(BLAS.extract_tri(3, 3, Triangle.Lower, Diagonal.NotUnitTri, init1)=[1.0,2.0,3.0,0.0,5.0,6.0,0.0,0.0,9.0]);
  END;
END;
