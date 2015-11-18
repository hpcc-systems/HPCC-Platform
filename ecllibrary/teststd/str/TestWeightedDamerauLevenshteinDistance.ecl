/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestWeightedDamerauLevenshteinDistance := MODULE

  STRING alpha := 'abcdefghijklmnopqrstuvwxyz';
  STRING digits := '0123456789';
  STRING largeAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.WeightedDamerauLevenshteinDistance('','') = 0, CONST);
    EXPORT Test02 := ASSERT(Str.WeightedDamerauLevenshteinDistance('','                ') = 0, CONST);
    EXPORT Test03 := ASSERT(Str.WeightedDamerauLevenshteinDistance('                ','') = 0, CONST);
    EXPORT Test04 := ASSERT(Str.WeightedDamerauLevenshteinDistance('a ','                ') = 1, CONST);
    //EXPORT Test05 := ASSERT(Str.WeightedDamerauLevenshteinDistance(' a ','   ') = 1, CONST);
    EXPORT Test06 := ASSERT(Str.WeightedDamerauLevenshteinDistance('Aprs  ','APp') = 2, CONST);//default confusion matrix is case insensitive.
    EXPORT Test07 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','acbd') = 1, CONST);
    EXPORT Test08 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','abd') = 1, CONST);
    EXPORT Test09 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','abc') = 1, CONST);
    EXPORT Test10 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','bcd') = 1, CONST);
    EXPORT Test11 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','abcde') = 1, CONST);
    EXPORT Test12 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','aabcd') = 1, CONST);
    EXPORT Test13 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd',' abcd') = 1, CONST);
    EXPORT Test14 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','a bcd') = 1, CONST);
    EXPORT Test15 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','adcd') = 0, CONST);
    EXPORT Test16 := ASSERT(Str.WeightedDamerauLevenshteinDistance('abcd','') = 4, CONST);
    EXPORT Test17 := ASSERT(Str.WeightedDamerauLevenshteinDistance(alpha,'') = 26, CONST);
    EXPORT Test18 := ASSERT(Str.WeightedDamerauLevenshteinDistance(manyAlpha,'') = 255, CONST);       //overflow
    EXPORT Test19 := ASSERT(Str.WeightedDamerauLevenshteinDistance(alpha,digits) = 26, CONST);
    EXPORT Test20 := ASSERT(Str.WeightedDamerauLevenshteinDistance(manyAlpha,digits) = 255, CONST);   //overflow
    EXPORT Test21 := ASSERT(Str.WeightedDamerauLevenshteinDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    EXPORT Test22 := ASSERT(Str.WeightedDamerauLevenshteinDistance(alpha,manyDigits) = 250, CONST);
    EXPORT Test23 := ASSERT(Str.WeightedDamerauLevenshteinDistance(alpha,manyDigits+'12345') = 255, CONST);
    EXPORT Test24 := ASSERT(Str.WeightedDamerauLevenshteinDistance(alpha,manyDigits+'123456') = 255, CONST);
    EXPORT Test25 := ASSERT(Str.WeightedDamerauLevenshteinDistance('123456789','987654321') = 8, CONST);
    EXPORT Test26 := ASSERT(Str.WeightedDamerauLevenshteinDistance(largeAlpha,manyDigits) = 250, CONST);  //overflow
    EXPORT Test27 := ASSERT(Str.WeightedDamerauLevenshteinDistance(largeAlpha+'abcdefghijklmnopqrst',manyDigits) = 254, CONST);
    EXPORT Test28 := ASSERT(Str.WeightedDamerauLevenshteinDistance(largeAlpha+'abcdefghijklmnopqrstu',manyDigits) = 255, CONST);
    EXPORT Test29 := ASSERT(Str.WeightedDamerauLevenshteinDistance(alpha,'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = 0, CONST);
    EXPORT Test30 := ASSERT(Str.WeightedDamerauLevenshteinDistance('ghgygtgfgvgb','bgvgfgtgyghg') = 0, CONST);
  END;

END;
