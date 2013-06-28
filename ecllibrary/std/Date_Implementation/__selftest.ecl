/*
 * Selftest code
 */

EXPORT __SelfTest := MODULE
  SHARED DateFormats := ['%d %b %Y', '%Y %b %d', '%Y%m%d', '%Y-%m-%d', '%d/%m/%Y'];

  EXPORT __TestConstant := [
    ASSERT(NOT IsLeapYear(1900), CONST);
    ASSERT(IsLeapYear(1904), CONST);
    ASSERT(NOT IsLeapYear(2100), CONST);
    ASSERT(IsLeapYear(2000), CONST);
    ASSERT(NOT IsLeapYear(1901), CONST);
    ASSERT(FromDaysSince1900(0) = 19000101, CONST);
    ASSERT(ToGregorianDate(1) = 00010101, CONST);
    ASSERT(DaysSince1900(1900,1,1)=0, CONST);
    ASSERT(FromGregorianYMD(1,1,1)=1, CONST);
    ASSERT(ToJulianDate(1) = 00010101, CONST);
    ASSERT(FromJulianYMD(1,1,1)=1, CONST);
    ASSERT(MonthsBetween(19700101,19701231)=11, CONST);
    ASSERT(MonthsBetween(19701231,19710101)=0, CONST);
    ASSERT(MonthsBetween(19701231,19711231)=12, CONST);
    ASSERT(MonthsBetween(19711231,19701231)=-12, CONST);
    ASSERT(MonthsBetween(19700606,19700706)=1, CONST);
    ASSERT(MonthsBetween(19700606,19700705)=0, CONST);
    ASSERT(MonthsBetween(19700606,19700607)=0, CONST);

    ASSERT(ToString(19700101, '%Y-%m-%d') = '1970-01-01', CONST);
    ASSERT(ToString(19700101, '%d/%m/%y') = '01/01/70', CONST);
    ASSERT(ToString(20110302, '%d %b %Y') = '02 Mar 2011', CONST);
    ASSERT(ToString(20111202, '%d %B %Y') = '02 December 2011', CONST);

    ASSERT(FromString('19700001', '%Y%m%d') = 0, CONST);
    ASSERT(FromString('19701000', '%Y%m%d') = 0, CONST);
    ASSERT(FromString('19700101', '%Y%m%d') = 19700101, CONST);
    ASSERT(FromString('68011', '%y%m%d') = 20680101, CONST);
    ASSERT(FromString('69011', '%y%m%d') = 19690101, CONST);
    ASSERT(FromString('1 \t Dec   2056', '%d %b %Y') = 20561201, CONST);
    ASSERT(FromString('1 \t December  1862', '%d %b %Y') = 18621201, CONST);
    ASSERT(FromString('31 \t jAN 12', '%d %b %Y') = 120131, CONST);
    ASSERT(FromString('1 \t De   2056', '%d %b %Y') = 0, CONST);
    ASSERT(FromString('1December1', '%d%b%Y') = 00011201, CONST);
//    ASSERT(MatchDateString('1dec2011',DateFormats) = 20111201, CONST);
    ASSERT(TRUE)
];

  EXPORT __TestDynamic := MODULE
    //Iterate through all lots of dates, incrementing the day and the date to check they convert correctly.
    Date_rec := Date_rec;
    test_rec := { Days_t day, Date_rec gregorian, Date_rec julian };
    firstDate := DATASET([{ 1, ROW(createDate(1,1,1)), ROW(createDate(1,1,1))}], test_rec);
    daysInLeapYearMonth := [31,29,31,30,31,30,31,31,30,31,30,31];
    daysInNonLeapYearMonth := [31,28,31,30,31,30,31,31,30,31,30,31];

    Date_rec makeNextDate(Date_rec prev, IsLeapYear leapYearFunc) := TRANSFORM
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
        SELF.gregorian := ROW(makeNextDate(prev.gregorian, IsLeapYear));
        SELF.julian := ROW(makeNextDate(prev.julian, IsJulianLeapYear));
    END;

    processNextDate(DATASET(test_rec) in) := FUNCTION
       next := PROJECT(in, nextRecord(LEFT));
       result1 := ASSERT(next, ToGregorianDate(next.day) = DateFromRec(next.gregorian));
       result2 := ASSERT(result1, next.day = FromGregorianDate(DateFromRec(next.gregorian)));
       result3 := ASSERT(result2, ToJulianDate(next.day) = DateFromRec(next.julian));
       result4 := ASSERT(result3, next.day = FromJulianDate(DateFromRec(next.julian)));
       RETURN result4;
    END;

    x := LOOP(firstDate, 1000000, processNextDate(ROWS(LEFT)));

    EXPORT __TestDynamic := [
      ASSERT(x[1].day = 1000001);
      ASSERT(MatchDateString('1dec2011',DateFormats) = 20111201);
      ASSERT(MatchDateString('2011dec1',DateFormats) = 20111201);
      ASSERT(MatchDateString('1 december 2011',DateFormats) = 20111201);
      ASSERT(MatchDateString('2011\tdecem\t01',DateFormats) = 20111201);
      ASSERT(MatchDateString('20111201',DateFormats) = 20111201);
      ASSERT(MatchDateString('2011-12-01',DateFormats) = 20111201);
      ASSERT(MatchDateString('1/12/2011',DateFormats) = 20111201);
      ASSERT(TRUE)
    ];
  END;

  EXPORT __SelfTest := [EVALUATE(__TestConstant), EVALUATE(__TestDynamic)];
END;
