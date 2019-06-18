/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestEditDistance := MODULE

  STRING alpha := 'abcdefghijklmnopqrstuvwxyz';
  STRING digits := '0123456789';
  STRING largeAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyAlpha := alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha+alpha;
  STRING manyDigits := digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+
                       digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits+digits;

  EXPORT TestConst := [
    ASSERT(Str.EditDistance('','') = 0, CONST);
    ASSERT(Str.EditDistance('','                ') = 0, CONST);
    ASSERT(Str.EditDistance('                ','') = 0, CONST);
    ASSERT(Str.EditDistance('a ','                ') = 1, CONST);
    //ASSERT(Str.EditDistance(' a ','   ') = 1, CONST);
    ASSERT(Str.EditDistance('Aprs  ','APp') = 3, CONST);
    ASSERT(Str.EditDistance('abcd','acbd') = 2, CONST);
    ASSERT(Str.EditDistance('abcd','abd') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','abc') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','bcd') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','abcde') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','aabcd') = 1, CONST);
    ASSERT(Str.EditDistance('abcd',' abcd') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','a bcd') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','adcd') = 1, CONST);
    ASSERT(Str.EditDistance('abcd','adca') = 2, CONST);
    ASSERT(Str.EditDistance('gavin','aving') = 2, CONST);
    ASSERT(Str.EditDistance('abcdefgh','cdefgha') = 3, CONST);
    ASSERT(Str.EditDistance('abcdefgh','abcdfgha') = 2, CONST);
    ASSERT(Str.EditDistance('abcd','') = 4, CONST);
    ASSERT(Str.EditDistance(alpha,'') = 26, CONST);
    ASSERT(Str.EditDistance(manyAlpha,'') = 255, CONST);       //overflow
    ASSERT(Str.EditDistance(alpha,digits) = 26, CONST);
    ASSERT(Str.EditDistance(manyAlpha,digits) = 255, CONST);   //overflow
    ASSERT(Str.EditDistance(manyAlpha,manyDigits) = 255, CONST);   //overflow
    ASSERT(Str.EditDistance(alpha,manyDigits) = 250, CONST);
    ASSERT(Str.EditDistance(alpha,manyDigits+'12345') = 255, CONST);
    ASSERT(Str.EditDistance(alpha,manyDigits+'123456') = 255, CONST);
    ASSERT(Str.EditDistance('123456789','987654321') = 8, CONST);
    ASSERT(Str.EditDistance(largeAlpha,manyDigits) = 250, CONST);  //overflow
    ASSERT(Str.EditDistance(largeAlpha+'abcdefghijklmnopqrst',manyDigits) = 254, CONST);
    ASSERT(Str.EditDistance(largeAlpha+'abcdefghijklmnopqrstu',manyDigits) = 255, CONST);

    ASSERT(Str.EditDistance('','', 1) = 0, CONST);
    ASSERT(Str.EditDistance('','                ', 1) = 0, CONST);
    ASSERT(Str.EditDistance('                ','', 1) = 0, CONST);
    ASSERT(Str.EditDistance('a ','                ', 1) = 1, CONST);
    //ASSERT(Str.EditDistance(' a ','   ', 1) = 1, CONST);
    ASSERT(Str.EditDistance('Aprs  ','APp', 1) > 1, CONST);
    ASSERT(Str.EditDistance('abcd','acbd', 1) = 2, CONST);
    ASSERT(Str.EditDistance('abcd','abd', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','abc', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','bcd', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','abcde', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','aabcd', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd',' abcd', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','a bcd', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','adcd', 1) = 1, CONST);
    ASSERT(Str.EditDistance('abcd','adca', 1) > 1, CONST);
    ASSERT(Str.EditDistance('gavin','aving', 1) > 1, CONST);
    ASSERT(Str.EditDistance('abcdefgh','cdefgha', 1) > 1, CONST);
    ASSERT(Str.EditDistance('abcdefgh','abcdfgha') > 1, CONST);
    ASSERT(Str.EditDistance('abcd','', 1) > 1, CONST);
    ASSERT(Str.EditDistance(alpha,'', 1) > 1, CONST);
    ASSERT(Str.EditDistance(manyAlpha,'', 1) > 1, CONST);       //overflow
    ASSERT(Str.EditDistance(alpha,digits, 1) > 1, CONST);
    ASSERT(Str.EditDistance(manyAlpha,digits, 1) > 1, CONST);   //overflow
    ASSERT(Str.EditDistance(manyAlpha,manyDigits, 1) > 1, CONST);   //overflow
    ASSERT(Str.EditDistance(alpha,manyDigits, 1) > 1, CONST);
    ASSERT(Str.EditDistance(alpha,manyDigits+'12345', 1) > 1, CONST);
    ASSERT(Str.EditDistance(alpha,manyDigits+'123456', 1) > 1, CONST);
    ASSERT(Str.EditDistance('123456789','987654321', 1) > 1, CONST);
    ASSERT(Str.EditDistance(largeAlpha,manyDigits, 1) > 1, CONST);  //overflow
    ASSERT(Str.EditDistance(largeAlpha+'abcdefghijklmnopqrst',manyDigits, 1) > 1, CONST);
    ASSERT(Str.EditDistance(largeAlpha+'abcdefghijklmnopqrstu',manyDigits, 1) > 1, CONST);

    EVALUATE('Done')
  ];

END;
