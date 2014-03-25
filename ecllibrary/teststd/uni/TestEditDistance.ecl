/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestEditDistance := MODULE

  UNICODE alpha := U'abcdefghijklmnopqrstuvwxyz';
  UNICODE digits := U'0123456789';
  UNICODE manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  UNICODE manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Uni.EditDistance(U'',U'') = 0, CONST);
    EXPORT Test02 := ASSERT(Uni.EditDistance(U'',U'                ') = 0, CONST);
    EXPORT Test03 := ASSERT(Uni.EditDistance(U'                ',U'') = 0, CONST);
    EXPORT Test04 := ASSERT(Uni.EditDistance(U'a ',U'                ') = 1, CONST);
    //EXPORT Test05 := ASSERT(Uni.EditDistance(U' a ',U'   ') = 1, CONST);
    EXPORT Test06 := ASSERT(Uni.EditDistance(U'Aprs  ',U'APp') = 3, CONST);
    EXPORT Test07 := ASSERT(Uni.EditDistance(U'abcd',U'acbd') = 2, CONST);
    EXPORT Test08 := ASSERT(Uni.EditDistance(U'abcd',U'abd') = 1, CONST);
    EXPORT Test09 := ASSERT(Uni.EditDistance(U'abcd',U'abc') = 1, CONST);
    EXPORT Test10 := ASSERT(Uni.EditDistance(U'abcd',U'bcd') = 1, CONST);
    EXPORT Test11 := ASSERT(Uni.EditDistance(U'abcd',U'abcde') = 1, CONST);
    EXPORT Test12 := ASSERT(Uni.EditDistance(U'abcd',U'aabcd') = 1, CONST);
    EXPORT Test13 := ASSERT(Uni.EditDistance(U'abcd',U' abcd') = 1, CONST);
    EXPORT Test14 := ASSERT(Uni.EditDistance(U'abcd',U'a bcd') = 1, CONST);
    EXPORT Test15 := ASSERT(Uni.EditDistance(U'abcd',U'adcd') = 1, CONST);
    EXPORT Test16 := ASSERT(Uni.EditDistance(U'abcd',U'') = 4, CONST);
    EXPORT Test17 := ASSERT(Uni.EditDistance(alpha,U'') = 26, CONST);
    EXPORT Test18 := ASSERT(Uni.EditDistance(manyAlpha,U'') = 255, CONST);      //overflow
    EXPORT Test19 := ASSERT(Uni.EditDistance(alpha,digits) = 26, CONST);
    EXPORT Test20 := ASSERT(Uni.EditDistance(manyAlpha,digits) = 255, CONST);   //overflow
    EXPORT Test21 := ASSERT(Uni.EditDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    EXPORT Test22 := ASSERT(Uni.EditDistance(alpha,manyDigits) = 250, CONST);
    EXPORT Test23 := ASSERT(Uni.EditDistance(alpha,manyDigits+U'12345') = 255, CONST);
    EXPORT Test24 := ASSERT(Uni.EditDistance(alpha,manyDigits+U'123456') = 255, CONST);
    EXPORT Test25 := ASSERT(Uni.EditDistance(U'123456789',U'987654321') = 8, CONST);
    EXPORT Test26 := ASSERT(Uni.EditDistance(U'AVILÉS',U'AVILES') = 1, CONST);
    EXPORT Test27 := ASSERT(Uni.EditDistance(U'MOMBRU',U'MOMBRÚ') = 1, CONST);
    EXPORT Test28 := ASSERT(Uni.EditDistance(U'ALVAREZ',U'ÁLVAREZ') = 1, CONST);
  END;

END;
