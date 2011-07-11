/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestEditDistanceWithinRadius := MODULE

  STRING alpha := 'abcdefghijklmnopqrstuvwxyz';
  STRING digits := '0123456789';
  STRING manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := MODULE
    EXPORT Test01a := ASSERT(Str.EditDistanceWithinRadius('','',0), CONST);
    EXPORT Test02a := ASSERT(Str.EditDistanceWithinRadius('','                ',0), CONST);
    EXPORT Test03a := ASSERT(Str.EditDistanceWithinRadius('                ','',0), CONST);
    EXPORT Test04a := ASSERT(Str.EditDistanceWithinRadius('a ','                ',1), CONST);
    EXPORT Test04b := ASSERT(NOT Str.EditDistanceWithinRadius('a ','                ',0), CONST);
//    EXPORT Test05a := ASSERT(Str.EditDistanceWithinRadius(' a ','  ', 1), CONST);
    EXPORT Test05b := ASSERT(NOT Str.EditDistanceWithinRadius(' a ','  ', 0), CONST);
    EXPORT Test06a := ASSERT(Str.EditDistanceWithinRadius('Aprs  ','APp',3), CONST);
    EXPORT Test06b := ASSERT(NOT Str.EditDistanceWithinRadius('Aprs  ','APp',2), CONST);
    EXPORT Test07a := ASSERT(Str.EditDistanceWithinRadius('abcd','acbd',2), CONST);
    EXPORT Test07b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','acbd',1), CONST);
    EXPORT Test08a := ASSERT(Str.EditDistanceWithinRadius('abcd','abd',1), CONST);
    EXPORT Test08b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','abd',0), CONST);
    EXPORT Test09a := ASSERT(Str.EditDistanceWithinRadius('abcd','abc',1), CONST);
    EXPORT Test09b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','abc',0), CONST);
    EXPORT Test10a := ASSERT(Str.EditDistanceWithinRadius('abcd','bcd',1), CONST);
    EXPORT Test10b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','bcd',0), CONST);
    EXPORT Test11a := ASSERT(Str.EditDistanceWithinRadius('abcd','abcde',1), CONST);
    EXPORT Test11b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','abcde',0), CONST);
    EXPORT Test12a := ASSERT(Str.EditDistanceWithinRadius('abcd','aabcd',1), CONST);
    EXPORT Test12b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','aabcd',0), CONST);
    EXPORT Test13a := ASSERT(Str.EditDistanceWithinRadius('abcd',' abcd',1), CONST);
    EXPORT Test13b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd',' abcd',0), CONST);
    EXPORT Test14a := ASSERT(Str.EditDistanceWithinRadius('abcd','a bcd',1), CONST);
    EXPORT Test14b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','a bcd',0), CONST);
    EXPORT Test15a := ASSERT(Str.EditDistanceWithinRadius('abcd','adcd',1), CONST);
    EXPORT Test15b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','adcd',0), CONST);
    EXPORT Test16a := ASSERT(Str.EditDistanceWithinRadius('abcd','',4), CONST);
    EXPORT Test16b := ASSERT(NOT Str.EditDistanceWithinRadius('abcd','',3), CONST);
    EXPORT Test17a := ASSERT(Str.EditDistanceWithinRadius(alpha,'',26), CONST);
    EXPORT Test17b := ASSERT(NOT Str.EditDistanceWithinRadius(alpha,'',25), CONST);
    EXPORT Test18a := ASSERT(Str.EditDistanceWithinRadius(manyAlpha,'',255), CONST);
    EXPORT Test18b := ASSERT(NOT Str.EditDistanceWithinRadius(manyAlpha,'',254), CONST);
    EXPORT Test19a := ASSERT(Str.EditDistanceWithinRadius(alpha,digits,26), CONST);
    EXPORT Test19b := ASSERT(NOT Str.EditDistanceWithinRadius(alpha,digits,25), CONST);
    EXPORT Test20a := ASSERT(Str.EditDistanceWithinRadius(manyAlpha,digits,255),CONST);         //overflow
    EXPORT Test20b := ASSERT(NOT Str.EditDistanceWithinRadius(manyAlpha,digits,254),CONST);     //overflow
    EXPORT Test21a := ASSERT(Str.EditDistanceWithinRadius(manyAlpha,manyDigits,255),CONST);     //overflow
    EXPORT Test21b := ASSERT(NOT Str.EditDistanceWithinRadius(manyAlpha,manyDigits,254),CONST); //overflow
    EXPORT Test22a := ASSERT(Str.EditDistanceWithinRadius(alpha,manyDigits,250), CONST);
    EXPORT Test22b := ASSERT(NOT Str.EditDistanceWithinRadius(alpha,manyDigits,249), CONST);
    EXPORT Test23a := ASSERT(Str.EditDistanceWithinRadius(alpha,manyDigits+'12345',255), CONST);
    EXPORT Test23b := ASSERT(NOT Str.EditDistanceWithinRadius(alpha,manyDigits+'12345',254), CONST);
    EXPORT Test24a := ASSERT(Str.EditDistanceWithinRadius(alpha,manyDigits+'123456',255), CONST);
    EXPORT Test24b := ASSERT(NOT Str.EditDistanceWithinRadius(alpha,manyDigits+'123456',254), CONST);
    EXPORT Test25a := ASSERT(Str.EditDistanceWithinRadius('123456789','987654321',8), CONST);
    EXPORT Test25b := ASSERT(NOT Str.EditDistanceWithinRadius('123456789','987654321',7), CONST);
  END;

END;
