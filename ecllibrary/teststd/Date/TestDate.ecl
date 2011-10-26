/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Date;

EXPORT TestDate := MODULE

  EXPORT TestConstant := [
    ASSERT(NOT date.IsLeapYear(1900), CONST);
    ASSERT(date.IsLeapYear(1904), CONST);
    ASSERT(NOT date.IsLeapYear(2100), CONST);
    ASSERT(date.IsLeapYear(2000), CONST);
    ASSERT(NOT date.IsLeapYear(1901), CONST);
    ASSERT(date.FromDaysSince1900(0) = 19000101, CONST);
    ASSERT(date.ToGregorianDate(1) = 00010101, CONST);
    ASSERT(date.DaysSince1900(1900,1,1)=0, CONST);
    ASSERT(date.FromGregorianYMD(1,1,1)=1, CONST);
    ASSERT(date.ToJulianDate(1) = 00010101, CONST);
    ASSERT(date.FromJulianYMD(1,1,1)=1, CONST);
    ASSERT(date.MonthsBetween(19700101,19701231)=11, CONST);
    ASSERT(date.MonthsBetween(19701231,19710101)=0, CONST);
    ASSERT(date.MonthsBetween(19701231,19711231)=12, CONST);
    ASSERT(date.MonthsBetween(19711231,19701231)=-12, CONST);
    ASSERT(date.MonthsBetween(19700606,19700706)=1, CONST);
    ASSERT(date.MonthsBetween(19700606,19700705)=0, CONST);
    ASSERT(date.MonthsBetween(19700606,19700607)=0, CONST);
    ASSERT(TRUE, CONST)
  ];

  EXPORT TestDynamic := MODULE
    //Iterate through all lots of dates, incrementing the day and the date to check they convert correctly.
    Date_rec := Date.Date_rec;
    test_rec := { Date.Days_t day, Date_rec gregorian, Date_rec julian };
    firstDate := DATASET([{ 1, ROW(Date.createDate(1,1,1)), ROW(Date.createDate(1,1,1))}], test_rec);
    daysInLeapYearMonth := [31,29,31,30,31,30,31,31,30,31,30,31];
    daysInNonLeapYearMonth := [31,28,31,30,31,30,31,31,30,31,30,31];

    Date_rec makeNextDate(Date_rec prev, Date.IsLeapYear leapYearFunc) := TRANSFORM
        daysInMonth(unsigned2 year) := IF(leapYearFunc(year), daysInLeapYearMonth, daysInNonLeapYearMonth);

        isLastDayOfMonth :=
               IF(leapYearFunc(prev.year),
                 prev.day = daysInLeapYearMonth[prev.month],
                 prev.day = daysInNonLeapYearMonth[prev.month]);
        isLastDayOfMonth2 := prev.day = daysInMonth(prev.year)[prev.month];
        isLastDayOfYear := prev.month=12 AND prev.day=31;

        SELF.year := IF(isLastDayOfYear, prev.year+1, prev.year);
        SELF.month := IF(isLastDayOfYear, 1, IF(isLastDayOfMonth, prev.month+1, prev.month));
        SELF.day := IF(isLastDayOfMonth, 1, prev.day+1);
    END;

    test_rec nextRecord(test_rec prev) := TRANSFORM
        SELF.day := prev.day + 1;
        SELF.gregorian := ROW(makeNextDate(prev.gregorian, Date.IsLeapYear));
        SELF.julian := ROW(makeNextDate(prev.julian, Date.IsJulianLeapYear));
    END;

    processNextDate(DATASET(test_rec) in) := FUNCTION
       next := PROJECT(in, nextRecord(LEFT));
       result1 := ASSERT(next, Date.ToGregorianDate(next.day) = Date.DateFromRec(next.gregorian));
       result2 := ASSERT(result1, next.day = Date.FromGregorianDate(Date.DateFromRec(next.gregorian)));
       result3 := ASSERT(result2, Date.ToJulianDate(next.day) = Date.DateFromRec(next.julian));
       result4 := ASSERT(result3, next.day = Date.FromJulianDate(Date.DateFromRec(next.julian)));
       RETURN result4;
    END;

    x := LOOP(firstDate, 1000000, processNextDate(ROWS(LEFT)));

    EXPORT Test01 := OUTPUT(x);
  END;

  EXPORT Main := [EVALUATE(TestConstant), EVALUATE(TestDynamic)];

END;
