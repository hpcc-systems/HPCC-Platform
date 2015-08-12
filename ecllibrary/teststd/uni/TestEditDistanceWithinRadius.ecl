/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
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
    EXPORT Test01b := ASSERT(Uni.EditDistanceWithinRadius(U'',U'',0,'en'), CONST);
    EXPORT Test02a := ASSERT(Uni.EditDistanceWithinRadius(U'',U'                ',0), CONST);
    EXPORT Test02b := ASSERT(Uni.EditDistanceWithinRadius(U'',U'                ',0,'en'), CONST);
    EXPORT Test03a := ASSERT(Uni.EditDistanceWithinRadius(U'                ',U'',0), CONST);
    EXPORT Test03b := ASSERT(Uni.EditDistanceWithinRadius(U'                ',U'',0,'en'), CONST);
    EXPORT Test04a := ASSERT(Uni.EditDistanceWithinRadius(U'a ',U'                ',1), CONST);
    EXPORT Test04b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'a ',U'                ',0), CONST);
    EXPORT Test04c := ASSERT(Uni.EditDistanceWithinRadius(U'a ',U'                ',1,'en'), CONST);
    EXPORT Test04d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'a ',U'                ',0,'en'), CONST);
//    EXPORT Test05a := ASSERT(Uni.EditDistanceWithinRadius(U' a ',U'  ', 1), CONST);
    EXPORT Test05b := ASSERT(NOT Uni.EditDistanceWithinRadius(U' a ',U'  ', 0), CONST);
    EXPORT Test05c := ASSERT(NOT Uni.EditDistanceWithinRadius(U' a ',U'  ', 0,'en'), CONST);
    EXPORT Test06a := ASSERT(Uni.EditDistanceWithinRadius(U'Aprs  ',U'APp',3), CONST);
    EXPORT Test06b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'Aprs  ',U'APp',2), CONST);
    EXPORT Test06c := ASSERT(Uni.EditDistanceWithinRadius(U'Aprs  ',U'APp',3,'en'), CONST);
    EXPORT Test06d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'Aprs  ',U'APp',2,'en'), CONST);
    EXPORT Test07a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'acbd',2), CONST);
    EXPORT Test07b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'acbd',1), CONST);
    EXPORT Test07c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'acbd',2,'en'), CONST);
    EXPORT Test07d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'acbd',1,'en'), CONST);
    EXPORT Test08a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abd',1), CONST);
    EXPORT Test08b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abd',0), CONST);
    EXPORT Test08c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abd',1,'en'), CONST);
    EXPORT Test08d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abd',0,'en'), CONST);
    EXPORT Test09a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abc',1), CONST);
    EXPORT Test09b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abc',0), CONST);
    EXPORT Test09c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abc',1,'en'), CONST);
    EXPORT Test09d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abc',0,'en'), CONST);
    EXPORT Test10a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'bcd',1), CONST);
    EXPORT Test10b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'bcd',0), CONST);
    EXPORT Test10c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'bcd',1,'en'), CONST);
    EXPORT Test10d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'bcd',0,'en'), CONST);
    EXPORT Test11a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abcde',1), CONST);
    EXPORT Test11b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abcde',0), CONST);
    EXPORT Test11c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'abcde',1,'en'), CONST);
    EXPORT Test11d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'abcde',0,'en'), CONST);
    EXPORT Test12a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'aabcd',1), CONST);
    EXPORT Test12b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'aabcd',0), CONST);
    EXPORT Test12c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'aabcd',1,'en'), CONST);
    EXPORT Test12d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'aabcd',0,'en'), CONST);
    EXPORT Test13a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U' abcd',1), CONST);
    EXPORT Test13b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U' abcd',0), CONST);
    EXPORT Test13c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U' abcd',1,'en'), CONST);
    EXPORT Test13d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U' abcd',0,'en'), CONST);
    EXPORT Test14a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'a bcd',1), CONST);
    EXPORT Test14b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'a bcd',0), CONST);
    EXPORT Test14c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'a bcd',1,'en'), CONST);
    EXPORT Test14d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'a bcd',0,'en'), CONST);
    EXPORT Test15a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'adcd',1), CONST);
    EXPORT Test15b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'adcd',0), CONST);
    EXPORT Test15c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'adcd',1,'en'), CONST);
    EXPORT Test15d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'adcd',0,'en'), CONST);
    EXPORT Test16a := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'',4), CONST);
    EXPORT Test16b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'',3), CONST);
    EXPORT Test16c := ASSERT(Uni.EditDistanceWithinRadius(U'abcd',U'',4,'en'), CONST);
    EXPORT Test16d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'abcd',U'',3,'en'), CONST);
    EXPORT Test17a := ASSERT(Uni.EditDistanceWithinRadius(alpha,U'',26), CONST);
    EXPORT Test17b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,U'',25), CONST);
    EXPORT Test17c := ASSERT(Uni.EditDistanceWithinRadius(alpha,U'',26,'en'), CONST);
    EXPORT Test17d := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,U'',25,'en'), CONST);
    EXPORT Test18a := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,U'',255), CONST);
    EXPORT Test18b := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,U'',254), CONST);
    EXPORT Test18c := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,U'',255,'en'), CONST);
    EXPORT Test18d := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,U'',254,'en'), CONST);
    EXPORT Test19a := ASSERT(Uni.EditDistanceWithinRadius(alpha,digits,26), CONST);
    EXPORT Test19b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,digits,25), CONST);
    EXPORT Test19c := ASSERT(Uni.EditDistanceWithinRadius(alpha,digits,26,'en'), CONST);
    EXPORT Test19d := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,digits,25,'en'), CONST);
    EXPORT Test20a := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,digits,255),CONST);         //overflow
    EXPORT Test20b := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,digits,254),CONST);     //overflow
    EXPORT Test20c := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,digits,255,'en'),CONST);         //overflow
    EXPORT Test20d := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,digits,254,'en'),CONST);     //overflow
    EXPORT Test21a := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,manyDigits,255),CONST);     //overflow
    EXPORT Test21b := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,manyDigits,254),CONST); //overflow
    EXPORT Test21c := ASSERT(Uni.EditDistanceWithinRadius(manyAlpha,manyDigits,255,'en'),CONST);     //overflow
    EXPORT Test21d := ASSERT(NOT Uni.EditDistanceWithinRadius(manyAlpha,manyDigits,254,'en'),CONST); //overflow
    EXPORT Test22a := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits,250), CONST);
    EXPORT Test22b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits,249), CONST);
    EXPORT Test22c := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits,250,'en'), CONST);
    EXPORT Test22d := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits,249,'en'), CONST);
    EXPORT Test23a := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits+U'12345',255), CONST);
    EXPORT Test23b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits+U'12345',254), CONST);
    EXPORT Test23c := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits+U'12345',255,'en'), CONST);
    EXPORT Test23d := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits+U'12345',254,'en'), CONST);
    EXPORT Test24a := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits+U'123456',255), CONST);
    EXPORT Test24b := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits+U'123456',254), CONST);
    EXPORT Test24c := ASSERT(Uni.EditDistanceWithinRadius(alpha,manyDigits+U'123456',255,'en'), CONST);
    EXPORT Test24d := ASSERT(NOT Uni.EditDistanceWithinRadius(alpha,manyDigits+U'123456',254,'en'), CONST);
    EXPORT Test25a := ASSERT(Uni.EditDistanceWithinRadius(U'123456789',U'987654321',8), CONST);
    EXPORT Test25b := ASSERT(NOT Uni.EditDistanceWithinRadius(U'123456789',U'987654321',7), CONST);
    EXPORT Test25c := ASSERT(Uni.EditDistanceWithinRadius(U'123456789',U'987654321',8,'en'), CONST);
    EXPORT Test25d := ASSERT(NOT Uni.EditDistanceWithinRadius(U'123456789',U'987654321',7,'en'), CONST);
    EXPORT Test26a := ASSERT(Uni.EditDistanceWithinRadius(U'AVILÉS',U'AVILES',1), CONST);
    EXPORT Test26b := ASSERT(Uni.EditDistanceWithinRadius(U'MOMBRU',U'MOMBRÚ',1), CONST);
    EXPORT Test26c := ASSERT(Uni.EditDistanceWithinRadius(U'BLVAREZ',U'ÁLVAREZ',1), CONST);
    EXPORT Test26aa := ASSERT(Uni.EditDistanceWithinRadius(U'AVILÉS',U'AVILES',1, 'en'), CONST);
    EXPORT Test26bb := ASSERT(Uni.EditDistanceWithinRadius(U'MOMBRU',U'MOMBRÚ',1, 'en'), CONST);
    EXPORT Test26cc := ASSERT(Uni.EditDistanceWithinRadius(U'BLVAREZ',U'ÁLVAREZ',1, 'en'), CONST);
    // when character's encoding is from 0x00ffff - 0x10ffff range: 0x1D306 ; Description=TETRAGRAM FOR CENTER (Tai Xuan Jing Symbols)
    // UTF-16 representation is xD834,xDF06 (2 16-bit surrogates)
    EXPORT Test27a := ASSERT(Uni.EditDistanceWithinRadius(U'\uD834\uDF06XXX',U'XXXX',1), CONST);
    EXPORT Test27b := ASSERT(Uni.EditDistanceWithinRadius(U'\uD834\uDF06XXX',U'XXXX',1, 'en'), CONST);
    // NFC (normalized form composed) for accented characters uses multiple 16-bit code units
    // for example: Ḍ̛ is encoded as 0x1E0C,0x031B, and Ḍ̛̇ as 0x1E0C,0x031B,0x0307
    // These are the cases where the fast function version (ToDo) does not work correctly, but this one does
    EXPORT Test28a := ASSERT(NOT Uni.EditDistanceWithinRadius(U'\u1E0C\u031BDDD',U'DDDD',1), CONST);
    EXPORT Test28b := ASSERT(Uni.EditDistanceWithinRadius(U'\u1E0C\u031BDDD',U'DDDD',1, 'en'), CONST);
    // Lithuanian 'i dot acute' is encoded as 0069 0307 0301
    EXPORT Test29a := ASSERT(NOT Uni.EditDistanceWithinRadius(U'\u0069\u0307\u0301DDD',U'DDDD',1), CONST);
    EXPORT Test29b := ASSERT(Uni.EditDistanceWithinRadius(U'\u0069\u0307\u0301DDD',U'DDDD',1, 'lt'), CONST);

  END;

END;
