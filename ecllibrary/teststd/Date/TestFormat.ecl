/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Date;

EXPORT TestFormat := MODULE

  SHARED DateFormats := ['%d %b %Y', '%Y %b %d', '%Y%m%d', '%Y-%m-%d', '%d/%m/%Y'];

  EXPORT TestConstant := [
    ASSERT(date.ToString(19700101, '%Y-%m-%d') = '1970-01-01', CONST);
    ASSERT(date.ToString(19700101, '%d/%m/%y') = '01/01/70', CONST);
    ASSERT(date.ToString(20110302, '%d %b %Y') = '02 Mar 2011', CONST);
    ASSERT(date.ToString(20111202, '%d %B %Y') = '02 December 2011', CONST);

    ASSERT(date.FromString('19700001', '%Y%m%d') = 0, CONST);
    ASSERT(date.FromString('19701000', '%Y%m%d') = 0, CONST);
    ASSERT(date.FromString('19700101', '%Y%m%d') = 19700101, CONST);
    ASSERT(date.FromString('68011', '%y%m%d') = 20680101, CONST);
    ASSERT(date.FromString('69011', '%y%m%d') = 19690101, CONST);
    ASSERT(date.FromString('1 \t Dec   2056', '%d %b %Y') = 20561201, CONST);
    ASSERT(date.FromString('1 \t December  1862', '%d %b %Y') = 18621201, CONST);
    ASSERT(date.FromString('31 \t jAN 12', '%d %b %Y') = 120131, CONST);
    ASSERT(date.FromString('1 \t De   2056', '%d %b %Y') = 0, CONST);
    ASSERT(date.FromString('1December1', '%d%b%Y') = 00011201, CONST);
//    ASSERT(date.MatchDateString('1dec2011',DateFormats) = 20111201, CONST);
    ASSERT(TRUE)
  ];

  EXPORT TestDynamic := [
    ASSERT(date.MatchDateString('1dec2011',DateFormats) = 20111201);
    ASSERT(date.MatchDateString('2011dec1',DateFormats) = 20111201);
    ASSERT(date.MatchDateString('1 december 2011',DateFormats) = 20111201);
    ASSERT(date.MatchDateString('2011\tdecem\t01',DateFormats) = 20111201);
    ASSERT(date.MatchDateString('20111201',DateFormats) = 20111201);
    ASSERT(date.MatchDateString('2011-12-01',DateFormats) = 20111201);
    ASSERT(date.MatchDateString('1/12/2011',DateFormats) = 20111201);
    ASSERT(TRUE)
  ];

  EXPORT Main := [EVALUATE(TestConstant), EVALUATE(TestDynamic)];

END;
