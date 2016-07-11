/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Date;

EXPORT TestFormat := MODULE

  SHARED DateFormats := ['%d %b %Y', '%Y %b %d', '%Y%m%d', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'];
  SHARED TimeFormats := ['%H%M%S', '%H:%M:%S', '%H:%M'];

  EXPORT TestConstant := [
    ASSERT(Date.FromStringToDate('19700001', '%Y%m%d') = 0, CONST);
    ASSERT(Date.FromStringToDate('19701000', '%Y%m%d') = 0, CONST);
    ASSERT(Date.FromStringToDate('19700101', '%Y%m%d') = 19700101, CONST);
    ASSERT(Date.FromStringToDate('68011', '%y%m%d') = 20680101, CONST);
    ASSERT(Date.FromStringToDate('69011', '%y%m%d') = 19690101, CONST);
    ASSERT(Date.FromStringToDate('1 \t Dec   2056', '%d %b %Y') = 20561201, CONST);
    ASSERT(Date.FromStringToDate('1 \t December  1862', '%d %b %Y') = 18621201, CONST);
    ASSERT(Date.FromStringToDate('31 \t jAN 12', '%d %b %Y') = 120131, CONST);
    ASSERT(Date.FromStringToDate('1 \t De   2056', '%d %b %Y') = 0, CONST);
    ASSERT(Date.FromStringToDate('1December1', '%d%b%Y') = 00011201, CONST);
    ASSERT(Date.FromStringToDate('1970-02-01', '%Y-%m-%d') = 19700201, CONST);

    ASSERT(Date.FromStringToTime('12:34:56', '%H:%M:%S') = 123456, CONST);

    ASSERT(TRUE)
  ];

  EXPORT TestDynamic := [
    ASSERT(Date.MatchDateString('1dec2011',DateFormats) = 20111201);
    ASSERT(Date.MatchDateString('2011dec1',DateFormats) = 20111201);
    ASSERT(Date.MatchDateString('1 december 2011',DateFormats) = 20111201);
    ASSERT(Date.MatchDateString('2011\tdecem\t01',DateFormats) = 20111201);
    ASSERT(Date.MatchDateString('20111201',DateFormats) = 20111201);
    ASSERT(Date.MatchDateString('2011-12-01',DateFormats) = 20111201);
    ASSERT(Date.MatchDateString('1/12/2011',DateFormats) = 20111201);

    ASSERT(Date.DateToString(19700101, '%Y-%m-%d') = '1970-01-01');
    ASSERT(Date.DateToString(19700101, '%d/%m/%y') = '01/01/70');
    ASSERT(Date.DateToString(20110302, '%d %b %Y') = '02 Mar 2011');
    ASSERT(Date.DateToString(20111202, '%d %B %Y') = '02 December 2011');

    ASSERT(Date.MatchTimeString('123456',TimeFormats) = 123456);
    ASSERT(Date.MatchTimeString('12:34:56',TimeFormats) = 123456);
    ASSERT(Date.MatchTimeString('12:34',TimeFormats) = 123400);

    ASSERT(Date.DateToString(19990201,'%Y-%m-%d') = '1999-02-01');

    ASSERT(Date.TimeToString(123456,'%H:%M:%S') = '12:34:56');

    ASSERT(Date.SecondsToString(917872496,'%Y-%m-%dT%H:%M:%S') = '1999-02-01T12:34:56');

    ASSERT(Date.ConvertDateFormat('1/12/2011','%m/%d/%Y','%Y-%m-%d') = '2011-01-12');

    ASSERT(Date.ConvertTimeFormat('123456','%H%M%S','%H:%M:%S') = '12:34:56');

    ASSERT(Date.ConvertDateFormatMultiple('1/31/2011',DateFormats,'%Y-%m-%d') = '2011-01-31');

    ASSERT(Date.ConvertDateFormatMultiple('',DateFormats,'%Y-%m-%d') = '');

    ASSERT(Date.ConvertTimeFormatMultiple('123456',TimeFormats,'%H:%M:%S') = '12:34:56');

    ASSERT(TRUE)
  ];

  EXPORT Main := [EVALUATE(TestConstant), EVALUATE(TestDynamic)];

END;
