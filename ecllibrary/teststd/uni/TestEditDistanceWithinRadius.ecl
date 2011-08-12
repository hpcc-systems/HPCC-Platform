/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestEditDistanceWithinRadius := MODULE

  UNICODE alpha := U'abcdefghijklmnopqrstuvwxyz';
  UNICODE digits := U'0123456789';
  UNICODE manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  UNICODE manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := MODULE
    EXPORT Test01a := ASSERT(Uni.EditDistanceWithinRadius(U'',U'',0), CONST);
    EXPORT Test02a := ASSERT(Uni.EditDistanceWithinRadius(U'',U'                ',0), CONST);
    EXPORT Test03a := ASSERT(Uni.EditDistanceWithinRadius(U'                ',U'',0), CONST);
    EXPORT Test04a := ASSERT(Uni.EditDistanceWithinRadius(U'a ',U'                ',1), CONST);
    EXPORT Test04b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'a ',U'                ',0), CONST);
//    EXPORT Test05a := ASSERT(Uni.EditDistanceWithinRadius(U' a ',U'  ', 1), CONST);
    EXPORT Test05b := ASSERT(NOT Uni.EditDistanceWithinRadius(U' a ',U'  ', 0), CONST);
    EXPORT Test06a := ASSERT(Uni.EditDistanceWithinRadius(U'Aprs  ',U'APp',3), CONST);
    EXPORT Test06b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'Aprs  ',U'APp',2), CONST);
    EXPORT Test07a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'acbd',2), CONST);
    EXPORT Test07b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'acbd',1), CONST);
    EXPORT Test08a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abd',1), CONST);
    EXPORT Test08b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abd',0), CONST);
    EXPORT Test09a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abc',1), CONST);
    EXPORT Test09b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abc',0), CONST);
    EXPORT Test10a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'bcd',1), CONST);
    EXPORT Test10b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'bcd',0), CONST);
    EXPORT Test11a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abcde',1), CONST);
    EXPORT Test11b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abcde',0), CONST);
    EXPORT Test12a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'aabcd',1), CONST);
    EXPORT Test12b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'aabcd',0), CONST);
    EXPORT Test13a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U' abcd',1), CONST);
    EXPORT Test13b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U' abcd',0), CONST);
    EXPORT Test14a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'a bcd',1), CONST);
    EXPORT Test14b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'a bcd',0), CONST);
    EXPORT Test15a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'adcd',1), CONST);
    EXPORT Test15b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'adcd',0), CONST);
    EXPORT Test16a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'',4), CONST);
    EXPORT Test16b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'',3), CONST);
    EXPORT Test17a := ASSERT(Uni.EditDistanceWithinRadius(alpha,U'',26), CONST);
    EXPORT Test17b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,U'',25), CONST);
    EXPORT Test18a := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,U'',255), CONST);
    EXPORT Test18b := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,U'',254), CONST);
    EXPORT Test19a := ASSERT(Uni.EditDistanceWithinRadius(alpha,digits,26), CONST);
    EXPORT Test19b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,digits,25), CONST);
    EXPORT Test20a := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,digits,255),CONST);         //overflow
    EXPORT Test20b := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,digits,254),CONST);     //overflow
    EXPORT Test21a := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,manyDigits,255),CONST);     //overflow
    EXPORT Test21b := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,manyDigits,254),CONST); //overflow
    EXPORT Test22a := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits,250), CONST);
    EXPORT Test22b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits,249), CONST);
    EXPORT Test23a := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits+U'12345',255), CONST);
    EXPORT Test23b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits+U'12345',254), CONST);
    EXPORT Test24a := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits+U'123456',255), CONST);
    EXPORT Test24b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits+U'123456',254), CONST);
    EXPORT Test25a := ASSERT(Uni.EditDistanceWithinRadius(U'123456789',U'987654321',8), CONST);
    EXPORT Test25b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'123456789',U'987654321',7), CONST);
  END;

END;
