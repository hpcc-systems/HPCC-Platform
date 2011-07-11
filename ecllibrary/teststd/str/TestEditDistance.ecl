/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestEditDistance := MODULE

  STRING alpha := 'abcdefghijklmnopqrstuvwxyz';
  STRING digits := '0123456789';
  STRING largeAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.EditDistance('','') = 0, CONST);
    EXPORT Test02 := ASSERT(Str.EditDistance('','                ') = 0, CONST);
    EXPORT Test03 := ASSERT(Str.EditDistance('                ','') = 0, CONST);
    EXPORT Test04 := ASSERT(Str.EditDistance('a ','                ') = 1, CONST);
    //EXPORT Test05 := ASSERT(Str.EditDistance(' a ','   ') = 1, CONST);
    EXPORT Test06 := ASSERT(Str.EditDistance('Aprs  ','APp') = 3, CONST);
    EXPORT Test07 := ASSERT(Str.EditDistance('abcd','acbd') = 2, CONST);
    EXPORT Test08 := ASSERT(Str.EditDistance('abcd','abd') = 1, CONST);
    EXPORT Test09 := ASSERT(Str.EditDistance('abcd','abc') = 1, CONST);
    EXPORT Test10 := ASSERT(Str.EditDistance('abcd','bcd') = 1, CONST);
    EXPORT Test11 := ASSERT(Str.EditDistance('abcd','abcde') = 1, CONST);
    EXPORT Test12 := ASSERT(Str.EditDistance('abcd','aabcd') = 1, CONST);
    EXPORT Test13 := ASSERT(Str.EditDistance('abcd',' abcd') = 1, CONST);
    EXPORT Test14 := ASSERT(Str.EditDistance('abcd','a bcd') = 1, CONST);
    EXPORT Test15 := ASSERT(Str.EditDistance('abcd','adcd') = 1, CONST);
    EXPORT Test16 := ASSERT(Str.EditDistance('abcd','') = 4, CONST);
    EXPORT Test17 := ASSERT(Str.EditDistance(alpha,'') = 26, CONST);
    EXPORT Test18 := ASSERT(Str.EditDistance(manyAlpha,'') = 255, CONST);       //overflow
    EXPORT Test19 := ASSERT(Str.EditDistance(alpha,digits) = 26, CONST);
    EXPORT Test20 := ASSERT(Str.EditDistance(manyAlpha,digits) = 255, CONST);   //overflow
    EXPORT Test21 := ASSERT(Str.EditDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    EXPORT Test22 := ASSERT(Str.EditDistance(alpha,manyDigits) = 250, CONST);
    EXPORT Test23 := ASSERT(Str.EditDistance(alpha,manyDigits+'12345') = 255, CONST);
    EXPORT Test24 := ASSERT(Str.EditDistance(alpha,manyDigits+'123456') = 255, CONST);
    EXPORT Test25 := ASSERT(Str.EditDistance('123456789','987654321') = 8, CONST);
    EXPORT Test26 := ASSERT(Str.EditDistance(largeAlpha,manyDigits) = 250, CONST);  //overflow
    EXPORT Test27 := ASSERT(Str.EditDistance(largeAlpha+'abcdefghijklmnopqrst',manyDigits) = 254, CONST);
    EXPORT Test28 := ASSERT(Str.EditDistance(largeAlpha+'abcdefghijklmnopqrstu',manyDigits) = 255, CONST);
  END;

END;
