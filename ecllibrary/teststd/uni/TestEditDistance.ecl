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

  EXPORT TestConst := [
    ASSERT(Uni.EditDistance(U'',U'') = 0, CONST);
    ASSERT(Uni.EditDistance(U'',U'','en') = 0, CONST);
    ASSERT(Uni.EditDistance(U'',U'                ') = 0, CONST);
    ASSERT(Uni.EditDistance(U'',U'                ','en') = 0, CONST);
    ASSERT(Uni.EditDistance(U'                ',U'') = 0, CONST);
    ASSERT(Uni.EditDistance(U'                ',U'','en') = 0, CONST);
    ASSERT(Uni.EditDistance(U'a ',U'                ') = 1, CONST);
    ASSERT(Uni.EditDistance(U'a ',U'                ','en') = 1, CONST);
    //ASSERT(Uni.EditDistance(U' a ',U'   ') = 1, CONST);
    ASSERT(Uni.EditDistance(U'Aprs  ',U'APp') = 3, CONST);
    ASSERT(Uni.EditDistance(U'Aprs  ',U'APp','en') = 3, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'acbd') = 2, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'acbd','en') = 2, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abd') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abd','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abc') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abc','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'bcd') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'bcd','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abcde') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abcde','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'aabcd') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'aabcd','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U' abcd') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U' abcd','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'a bcd') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'a bcd','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'adcd') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'adcd','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'') = 4, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'','en') = 4, CONST);
    ASSERT(Uni.EditDistance(U'gavin',U'aving') = 2, CONST);
    ASSERT(Uni.EditDistance(U'abcdefgh',U'cdefgha') = 3, CONST);
    ASSERT(Uni.EditDistance(U'abcdefgh',U'abcdfgha') = 2, CONST);
    ASSERT(Uni.EditDistance(alpha,U'') = 26, CONST);
    ASSERT(Uni.EditDistance(alpha,U'','en') = 26, CONST);
    ASSERT(Uni.EditDistance(manyAlpha,U'') = 255, CONST);      //overflow
    ASSERT(Uni.EditDistance(manyAlpha,U'','en') = 255, CONST);      //overflow
    ASSERT(Uni.EditDistance(alpha,digits) = 26, CONST);
    ASSERT(Uni.EditDistance(alpha,digits,'en') = 26, CONST);
    ASSERT(Uni.EditDistance(manyAlpha,digits) = 255, CONST);   //overflow
    ASSERT(Uni.EditDistance(manyAlpha,digits,'en') = 255, CONST);   //overflow
    ASSERT(Uni.EditDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    ASSERT(Uni.EditDistance(manyAlpha,manyDigits,'en') = 255, CONST);   //overflow
    ASSERT(Uni.EditDistance(alpha,manyDigits) = 250, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits,'en') = 250, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits+U'12345') = 255, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits+U'12345','en') = 255, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits+U'123456') = 255, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits+U'123456','en') = 255, CONST);
    ASSERT(Uni.EditDistance(U'123456789',U'987654321') = 8, CONST);
    ASSERT(Uni.EditDistance(U'123456789',U'987654321','en') = 8, CONST);
    ASSERT(Uni.EditDistance(U'AVILÉS',U'AVILES') = 1, CONST);
    ASSERT(Uni.EditDistance(U'AVILÉS',U'AVILES','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'MOMBRU',U'MOMBRÚ') = 1, CONST);
    ASSERT(Uni.EditDistance(U'MOMBRU',U'MOMBRÚ','en') = 1, CONST);
    ASSERT(Uni.EditDistance(U'BLVAREZ',U'ÁLVAREZ') = 1, CONST);
    ASSERT(Uni.EditDistance(U'BLVAREZ',U'ÁLVAREZ','en') = 1, CONST);
    // when character's encoding is from 0x00ffff - 0x10ffff range: 0x1D306 ; Description=TETRAGRAM FOR CENTER (Tai Xuan Jing Symbols)
    // UTF-16 representation is xD834,xDF06 (2 16-bit surrogates)
    ASSERT(Uni.EditDistance(U'\uD834\uDF06XXX',U'XXXX') = 1, CONST);
    ASSERT(Uni.EditDistance(U'\uD834\uDF06XXX',U'XXXX','en') = 1, CONST);
    // NFC (normalized form composed) for accented characters uses multiple 16-bit code units
    // for example: Ḍ̛ is encoded as 0x1E0C,0x031B, and Ḍ̛̇ as 0x1E0C,0x031B,0x0307
    // These are the cases where the fast function version (ToDo) does not work correctly, but this one does
    ASSERT(Uni.EditDistance(U'\u1E0C\u031BDDD',U'DDDD') = 2, CONST);
    ASSERT(Uni.EditDistance(U'\u1E0C\u031BDDD',U'DDDD','en') = 1, CONST);
    // Lithuanian 'i dot acute' is encoded as 0069 0307 0301
    ASSERT(Uni.EditDistance(U'\u0069\u0307\u0301DDD',U'DDDD') = 3, CONST);
    ASSERT(Uni.EditDistance(U'\u0069\u0307\u0301DDD',U'DDDD','lt') = 1, CONST);

    ASSERT(Uni.EditDistance(U'',U'','en', 1) = 0, CONST);
    ASSERT(Uni.EditDistance(U'',U'                ','en', 1) = 0, CONST);
    ASSERT(Uni.EditDistance(U'                ',U'','en', 1) = 0, CONST);
    ASSERT(Uni.EditDistance(U'a ',U'                ','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'Aprs  ',U'APp','en', 1) = 3, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'acbd','en', 1) = 2, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abd','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abc','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'bcd','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'abcde','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'aabcd','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U' abcd','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'a bcd','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'adcd','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'abcd',U'','en', 1) = 4, CONST);
    ASSERT(Uni.EditDistance(U'gavin',U'aving', 'en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(U'abcdefgh',U'cdefgha', 'en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(U'abcdefgh',U'abcdfgha', 'en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(alpha,U'','en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(manyAlpha,U'','en', 1) > 1, CONST);      //overflow
    ASSERT(Uni.EditDistance(alpha,digits,'en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(manyAlpha,digits,'en', 1) > 1, CONST);   //overflow
    ASSERT(Uni.EditDistance(manyAlpha,manyDigits,'en', 1) > 1, CONST);   //overflow
    ASSERT(Uni.EditDistance(alpha,manyDigits,'en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits+U'12345','en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(alpha,manyDigits+U'123456','en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(U'123456789',U'987654321','en', 1) > 1, CONST);
    ASSERT(Uni.EditDistance(U'123456789',U'987654321','en', 7) > 7, CONST);
    ASSERT(Uni.EditDistance(U'123456789',U'987654321','en', 8) = 8, CONST);
    ASSERT(Uni.EditDistance(U'AVILÉS',U'AVILES','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'MOMBRU',U'MOMBRÚ','en', 1) = 1, CONST);
    ASSERT(Uni.EditDistance(U'BLVAREZ',U'ÁLVAREZ','en', 1) = 1, CONST);
    // when character's encoding is from 0x00ffff - 0x10ffff range: 0x1D306 ; Description=TETRAGRAM FOR CENTER (Tai Xuan Jing Symbols)
    // UTF-16 representation is xD834,xDF06 (2 16-bit surrogates)
    ASSERT(Uni.EditDistance(U'\uD834\uDF06XXX',U'XXXX','en', 1) = 1, CONST);
    // NFC (normalized form composed, 1) for accented characters uses multiple 16-bit code units
    // for example: Ḍ̛ is encoded as 0x1E0C,0x031B, and Ḍ̛̇ as 0x1E0C,0x031B,0x0307
    // These are the cases where the fast function version (ToDo, 1) does not work correctly, but this one does
    ASSERT(Uni.EditDistance(U'\u1E0C\u031BDDD',U'DDDD','en', 1) = 1, CONST);
    // Lithuanian 'i dot acute' is encoded as 0069 0307 0301
    ASSERT(Uni.EditDistance(U'\u0069\u0307\u0301DDD',U'DDDD','lt', 1) = 1, CONST);

    EVALUATE('Done')
  ];

END;
