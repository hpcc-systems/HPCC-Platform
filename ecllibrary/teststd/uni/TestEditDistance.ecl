/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
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
    EXPORT Test01a := ASSERT(Uni.EditDistance(U'',U'','en') = 0, CONST);
    EXPORT Test02 := ASSERT(Uni.EditDistance(U'',U'                ') = 0, CONST);
    EXPORT Test02a := ASSERT(Uni.EditDistance(U'',U'                ','en') = 0, CONST);
    EXPORT Test03 := ASSERT(Uni.EditDistance(U'                ',U'') = 0, CONST);
    EXPORT Test03a := ASSERT(Uni.EditDistance(U'                ',U'','en') = 0, CONST);
    EXPORT Test04 := ASSERT(Uni.EditDistance(U'a ',U'                ') = 1, CONST);
    EXPORT Test04a := ASSERT(Uni.EditDistance(U'a ',U'                ','en') = 1, CONST);
    //EXPORT Test05 := ASSERT(Uni.EditDistance(U' a ',U'   ') = 1, CONST);
    EXPORT Test06 := ASSERT(Uni.EditDistance(U'Aprs  ',U'APp') = 3, CONST);
    EXPORT Test06a := ASSERT(Uni.EditDistance(U'Aprs  ',U'APp','en') = 3, CONST);
    EXPORT Test07 := ASSERT(Uni.EditDistance(U'abcd',U'acbd') = 2, CONST);
    EXPORT Test07a := ASSERT(Uni.EditDistance(U'abcd',U'acbd','en') = 2, CONST);
    EXPORT Test08 := ASSERT(Uni.EditDistance(U'abcd',U'abd') = 1, CONST);
    EXPORT Test08a := ASSERT(Uni.EditDistance(U'abcd',U'abd','en') = 1, CONST);
    EXPORT Test09 := ASSERT(Uni.EditDistance(U'abcd',U'abc') = 1, CONST);
    EXPORT Test09a := ASSERT(Uni.EditDistance(U'abcd',U'abc','en') = 1, CONST);
    EXPORT Test10 := ASSERT(Uni.EditDistance(U'abcd',U'bcd') = 1, CONST);
    EXPORT Test10a := ASSERT(Uni.EditDistance(U'abcd',U'bcd','en') = 1, CONST);
    EXPORT Test11 := ASSERT(Uni.EditDistance(U'abcd',U'abcde') = 1, CONST);
    EXPORT Test11a := ASSERT(Uni.EditDistance(U'abcd',U'abcde','en') = 1, CONST);
    EXPORT Test12 := ASSERT(Uni.EditDistance(U'abcd',U'aabcd') = 1, CONST);
    EXPORT Test12a := ASSERT(Uni.EditDistance(U'abcd',U'aabcd','en') = 1, CONST);
    EXPORT Test13 := ASSERT(Uni.EditDistance(U'abcd',U' abcd') = 1, CONST);
    EXPORT Test13a := ASSERT(Uni.EditDistance(U'abcd',U' abcd','en') = 1, CONST);
    EXPORT Test14 := ASSERT(Uni.EditDistance(U'abcd',U'a bcd') = 1, CONST);
    EXPORT Test14a := ASSERT(Uni.EditDistance(U'abcd',U'a bcd','en') = 1, CONST);
    EXPORT Test15 := ASSERT(Uni.EditDistance(U'abcd',U'adcd') = 1, CONST);
    EXPORT Test15a := ASSERT(Uni.EditDistance(U'abcd',U'adcd','en') = 1, CONST);
    EXPORT Test16 := ASSERT(Uni.EditDistance(U'abcd',U'') = 4, CONST);
    EXPORT Test16a := ASSERT(Uni.EditDistance(U'abcd',U'','en') = 4, CONST);
    EXPORT Test17 := ASSERT(Uni.EditDistance(alpha,U'') = 26, CONST);
    EXPORT Test17a := ASSERT(Uni.EditDistance(alpha,U'','en') = 26, CONST);
    EXPORT Test18 := ASSERT(Uni.EditDistance(manyAlpha,U'') = 255, CONST);      //overflow
    EXPORT Test18a := ASSERT(Uni.EditDistance(manyAlpha,U'','en') = 255, CONST);      //overflow
    EXPORT Test19 := ASSERT(Uni.EditDistance(alpha,digits) = 26, CONST);
    EXPORT Test19a := ASSERT(Uni.EditDistance(alpha,digits,'en') = 26, CONST);
    EXPORT Test20 := ASSERT(Uni.EditDistance(manyAlpha,digits) = 255, CONST);   //overflow
    EXPORT Test20a := ASSERT(Uni.EditDistance(manyAlpha,digits,'en') = 255, CONST);   //overflow
    EXPORT Test21 := ASSERT(Uni.EditDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    EXPORT Test21a := ASSERT(Uni.EditDistance(manyAlpha,manyDigits,'en') = 255, CONST);   //overflow
    EXPORT Test22 := ASSERT(Uni.EditDistance(alpha,manyDigits) = 250, CONST);
    EXPORT Test22a := ASSERT(Uni.EditDistance(alpha,manyDigits,'en') = 250, CONST);
    EXPORT Test23 := ASSERT(Uni.EditDistance(alpha,manyDigits+U'12345') = 255, CONST);
    EXPORT Test23a := ASSERT(Uni.EditDistance(alpha,manyDigits+U'12345','en') = 255, CONST);
    EXPORT Test24 := ASSERT(Uni.EditDistance(alpha,manyDigits+U'123456') = 255, CONST);
    EXPORT Test24a := ASSERT(Uni.EditDistance(alpha,manyDigits+U'123456','en') = 255, CONST);
    EXPORT Test25 := ASSERT(Uni.EditDistance(U'123456789',U'987654321') = 8, CONST);
    EXPORT Test25a := ASSERT(Uni.EditDistance(U'123456789',U'987654321','en') = 8, CONST);
    EXPORT Test26 := ASSERT(Uni.EditDistance(U'AVILÉS',U'AVILES') = 1, CONST);
    EXPORT Test26a := ASSERT(Uni.EditDistance(U'AVILÉS',U'AVILES','en') = 1, CONST);
    EXPORT Test27 := ASSERT(Uni.EditDistance(U'MOMBRU',U'MOMBRÚ') = 1, CONST);
    EXPORT Test27a := ASSERT(Uni.EditDistance(U'MOMBRU',U'MOMBRÚ','en') = 1, CONST);
    EXPORT Test28 := ASSERT(Uni.EditDistance(U'BLVAREZ',U'ÁLVAREZ') = 1, CONST);
    EXPORT Test28a := ASSERT(Uni.EditDistance(U'BLVAREZ',U'ÁLVAREZ','en') = 1, CONST);
    // when character's encoding is from 0x00ffff - 0x10ffff range: 0x1D306 ; Description=TETRAGRAM FOR CENTER (Tai Xuan Jing Symbols)
    // UTF-16 representation is xD834,xDF06 (2 16-bit surrogates)
    EXPORT Test29 := ASSERT(Uni.EditDistance(U'\uD834\uDF06XXX',U'XXXX') = 1, CONST);
    EXPORT Test29a := ASSERT(Uni.EditDistance(U'\uD834\uDF06XXX',U'XXXX','en') = 1, CONST);
    // NFC (normalized form composed) for accented characters uses multiple 16-bit code units
    // for example: Ḍ̛ is encoded as 0x1E0C,0x031B, and Ḍ̛̇ as 0x1E0C,0x031B,0x0307
    // These are the cases where the fast function version (ToDo) does not work correctly, but this one does
    EXPORT Test30 := ASSERT(Uni.EditDistance(U'\u1E0C\u031BDDD',U'DDDD') = 2, CONST);
    EXPORT Test30a := ASSERT(Uni.EditDistance(U'\u1E0C\u031BDDD',U'DDDD','en') = 1, CONST);
    // Lithuanian 'i dot acute' is encoded as 0069 0307 0301
    EXPORT Test31 := ASSERT(Uni.EditDistance(U'\u0069\u0307\u0301DDD',U'DDDD') = 3, CONST);
    EXPORT Test31a := ASSERT(Uni.EditDistance(U'\u0069\u0307\u0301DDD',U'DDDD','lt') = 1, CONST);
  END;
END;
