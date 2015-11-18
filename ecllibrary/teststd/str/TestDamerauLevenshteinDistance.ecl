/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestDamerauLevenshteinDistance := MODULE

  STRING alpha := 'abcdefghijklmnopqrstuvwxyz';
  STRING digits := '0123456789';
  STRING largeAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.DamerauLevenshteinDistance('','') = 0, CONST);
    EXPORT Test02 := ASSERT(Str.DamerauLevenshteinDistance('','                ') = 0, CONST);
    EXPORT Test03 := ASSERT(Str.DamerauLevenshteinDistance('                ','') = 0, CONST);
    EXPORT Test04 := ASSERT(Str.DamerauLevenshteinDistance('a ','                ') = 1, CONST);
    //EXPORT Test05 := ASSERT(Str.DamerauLevenshteinDistance(' a ','   ') = 1, CONST);
    EXPORT Test06 := ASSERT(Str.DamerauLevenshteinDistance('Aprs  ','APp') = 3, CONST);
    EXPORT Test07 := ASSERT(Str.DamerauLevenshteinDistance('abcd','acbd') = 1, CONST);
    EXPORT Test08 := ASSERT(Str.DamerauLevenshteinDistance('abcd','abd') = 1, CONST);
    EXPORT Test09 := ASSERT(Str.DamerauLevenshteinDistance('abcd','abc') = 1, CONST);
    EXPORT Test10 := ASSERT(Str.DamerauLevenshteinDistance('abcd','bcd') = 1, CONST);
    EXPORT Test11 := ASSERT(Str.DamerauLevenshteinDistance('abcd','abcde') = 1, CONST);
    EXPORT Test12 := ASSERT(Str.DamerauLevenshteinDistance('abcd','aabcd') = 1, CONST);
    EXPORT Test13 := ASSERT(Str.DamerauLevenshteinDistance('abcd',' abcd') = 1, CONST);
    EXPORT Test14 := ASSERT(Str.DamerauLevenshteinDistance('abcd','a bcd') = 1, CONST);
    EXPORT Test15 := ASSERT(Str.DamerauLevenshteinDistance('abcd','adcd') = 1, CONST);
    EXPORT Test16 := ASSERT(Str.DamerauLevenshteinDistance('abcd','') = 4, CONST);
    EXPORT Test17 := ASSERT(Str.DamerauLevenshteinDistance(alpha,'') = 26, CONST);
    EXPORT Test18 := ASSERT(Str.DamerauLevenshteinDistance(manyAlpha,'') = 255, CONST);       //overflow
    EXPORT Test19 := ASSERT(Str.DamerauLevenshteinDistance(alpha,digits) = 26, CONST);
    EXPORT Test20 := ASSERT(Str.DamerauLevenshteinDistance(manyAlpha,digits) = 255, CONST);   //overflow
    EXPORT Test21 := ASSERT(Str.DamerauLevenshteinDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    EXPORT Test22 := ASSERT(Str.DamerauLevenshteinDistance(alpha,manyDigits) = 250, CONST);
    EXPORT Test23 := ASSERT(Str.DamerauLevenshteinDistance(alpha,manyDigits+'12345') = 255, CONST);
    EXPORT Test24 := ASSERT(Str.DamerauLevenshteinDistance(alpha,manyDigits+'123456') = 255, CONST);
    EXPORT Test25 := ASSERT(Str.DamerauLevenshteinDistance('123456789','987654321') = 8, CONST);
    EXPORT Test26 := ASSERT(Str.DamerauLevenshteinDistance(largeAlpha,manyDigits) = 250, CONST);  //overflow
    EXPORT Test27 := ASSERT(Str.DamerauLevenshteinDistance(largeAlpha+'abcdefghijklmnopqrst',manyDigits) = 254, CONST);
    EXPORT Test28 := ASSERT(Str.DamerauLevenshteinDistance(largeAlpha+'abcdefghijklmnopqrstu',manyDigits) = 255, CONST);
    EXPORT Test29 := ASSERT(Str.DamerauLevenshteinDistance(alpha,'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = 26, CONST);
    EXPORT Test30 := ASSERT(Str.DamerauLevenshteinDistance('ghgygtgfgvgb','bgvgfgtgyghg') = 6, CONST);
    EXPORT Test31 := ASSERT(Str.DamerauLevenshteinDistance('ca','abc') = 2, CONST);
    EXPORT Test32 := ASSERT(Str.DamerauLevenshteinDistance('abcd','abdc') = 1, CONST);
  END;

END;
