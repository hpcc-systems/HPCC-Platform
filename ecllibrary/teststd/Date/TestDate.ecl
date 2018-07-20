/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;
IMPORT Std.Date;

EXPORT TestDate := MODULE

  // Note:  Cannot perform static local time zone checks because success depends
  // on the time zone of the cluster

  SHARED vCreateDateTimeFromSeconds := ROW(Date.CreateDateTimeFromSeconds(917872496)); // Feb 1, 1999 @ 12:34:56 (UTC)
  SHARED vCreateDateFromSeconds := ROW(Date.CreateDateFromSeconds(917872496)); // Feb 1, 1999 (UTC)
  SHARED vCreateTimeFromSeconds := ROW(Date.CreateTimeFromSeconds(917872496)); // 12:34:56 (UTC)
  SHARED vCreateTime := ROW(Date.CreateTime(12,34,56)); // 12:34:56
  SHARED vCreateDateTime := ROW(Date.CreateDateTime(1999,2,1,12,34,56)); // Feb 1, 1999 @ 12:34:56
  SHARED vDate := Date.CurrentDate(); // UTC
  SHARED vToday := Date.Today(); // Local
  SHARED vTime := Date.CurrentTime(); // UTC
  SHARED vTimeLocal := Date.CurrentTime(TRUE); // Local
  SHARED vSeconds := Date.CurrentSeconds(); // UTC
  SHARED vSecondsLocal := Date.CurrentSeconds(TRUE); // Local
  SHARED vIndTimestamp := Date.CurrentTimestamp() : INDEPENDENT; // UTC, evaluated before all others
  SHARED vTimestamp := Date.CurrentTimestamp(); // UTC
  SHARED vTimestampLocal := Date.CurrentTimestamp(TRUE); // Local
  SHARED vLocalTimeZoneOffset := Date.LocalTimeZoneOffset();
  SHARED vTZInputSample := NOFOLD(DATASET([{120000, 'CT'}], {Date.Time_t time, STRING tz}));

  EXPORT TestConstant := [
    ASSERT(Date.FromDaysSince1900(0) = 19000101, CONST);
    ASSERT(Date.ToGregorianDate(1) = 00010101, CONST);
    ASSERT(Date.DaysSince1900(1900,1,1)=0, CONST);
    ASSERT(Date.DaysSince1900(1900,12,0)=334, CONST);
    ASSERT(Date.DaysSince1900(1900,12,1)=334, CONST);
    ASSERT(Date.DaysSince1900(1900,12,31)=364, CONST);
    ASSERT(Date.FromGregorianYMD(1,1,1)=1, CONST);
    ASSERT(Date.FromGregorianYMD(1,0,0)=1, CONST);
    ASSERT(Date.ToJulianDate(1) = 00010101, CONST);
    ASSERT(Date.FromJulianYMD(1,1,1)=1, CONST);

    ASSERT(Date.Year(19990201) = 1999, CONST);
    ASSERT(Date.Month(19990201) = 2, CONST);
    ASSERT(Date.Day(19990201) = 1, CONST);

    ASSERT(Date.Hour(123456) = 12, CONST);
    ASSERT(Date.Minute(123456) = 34, CONST);
    ASSERT(Date.Second(123456) = 56, CONST);

    ASSERT(Date.DateFromParts(1999,2,1) = 19990201, CONST);
    ASSERT(Date.TimeFromParts(12,34,56) = 123456, CONST);

    ASSERT(NOT Date.IsLeapYear(1900), CONST);
    ASSERT(Date.IsLeapYear(1904), CONST);
    ASSERT(NOT Date.IsLeapYear(2100), CONST);
    ASSERT(Date.IsLeapYear(2000), CONST);
    ASSERT(NOT Date.IsLeapYear(1901), CONST);

    ASSERT(Date.IsDateLeapYear(20000201) = TRUE, CONST);
    ASSERT(Date.IsDateLeapYear(20010201) = FALSE, CONST);
    ASSERT(Date.IsDateLeapYear(21000201) = FALSE, CONST);

    ASSERT(Date.IsJulianLeapYear(2000) = TRUE, CONST);
    ASSERT(Date.IsJulianLeapYear(2001) = FALSE, CONST);

    ASSERT(Date.YearsBetween(20010615,20020615) = 1, CONST);
    ASSERT(Date.YearsBetween(20010615,20020614) = 0, CONST);
    ASSERT(Date.YearsBetween(20020615,20010615) = -1, CONST);

    ASSERT(Date.MonthsBetween(19700101,19701231)=11, CONST);
    ASSERT(Date.MonthsBetween(19701231,19710101)=0, CONST);
    ASSERT(Date.MonthsBetween(19701231,19711231)=12, CONST);
    ASSERT(Date.MonthsBetween(19711231,19701231)=-12, CONST);
    ASSERT(Date.MonthsBetween(19700606,19700706)=1, CONST);
    ASSERT(Date.MonthsBetween(19700606,19700705)=0, CONST);
    ASSERT(Date.MonthsBetween(19700606,19700607)=0, CONST);
    // HPCC-16520
    ASSERT(Date.MonthsBetween(20160331,20160930)=5, CONST);
    ASSERT(Date.MonthsBetween(20160331,20160930,TRUE)=6, CONST);

    ASSERT(Date.DaysBetween(20010615,20020615) = 365, CONST);
    ASSERT(Date.DaysBetween(20010615,20020614) = 364, CONST);
    ASSERT(Date.DaysBetween(20020615,20010615) = -365, CONST);

    ASSERT(Date.YearWeekNumFromDate(20180101) = 1, CONST);      // Start of week = Sunday
    ASSERT(Date.YearWeekNumFromDate(20180106) = 1, CONST);      // Start of week = Sunday
    ASSERT(Date.YearWeekNumFromDate(20180107) = 2, CONST);      // Start of week = Sunday
    ASSERT(Date.YearWeekNumFromDate(20180107, 2) = 1, CONST);   // Start of week = Monday
    ASSERT(Date.YearWeekNumFromDate(20180108, 2) = 2, CONST);   // Start of week = Monday
    ASSERT(Date.YearWeekNumFromDate(20180101, 3) = 1, CONST);   // Start of week = Tuesday
    ASSERT(Date.YearWeekNumFromDate(20180102, 3) = 2, CONST);   // Start of week = Tuesday
    ASSERT(Date.YearWeekNumFromDate(20180102, 4) = 1, CONST);   // Start of week = Wednesday
    ASSERT(Date.YearWeekNumFromDate(20180103, 4) = 2, CONST);   // Start of week = Wednesday
    ASSERT(Date.YearWeekNumFromDate(20180103, 5) = 1, CONST);   // Start of week = Thursday
    ASSERT(Date.YearWeekNumFromDate(20180104, 5) = 2, CONST);   // Start of week = Thursday
    ASSERT(Date.YearWeekNumFromDate(20180104, 6) = 1, CONST);   // Start of week = Friday
    ASSERT(Date.YearWeekNumFromDate(20180105, 6) = 2, CONST);   // Start of week = Friday
    ASSERT(Date.YearWeekNumFromDate(20180105, 7) = 1, CONST);   // Start of week = Saturday
    ASSERT(Date.YearWeekNumFromDate(20180106, 7) = 2, CONST);   // Start of week = Saturday

    ASSERT(Date.MonthWeekNumFromDate(20180101) = 1, CONST);     // Start of week = Sunday
    ASSERT(Date.MonthWeekNumFromDate(20180106) = 1, CONST);     // Start of week = Sunday
    ASSERT(Date.MonthWeekNumFromDate(20180107) = 2, CONST);     // Start of week = Sunday
    ASSERT(Date.MonthWeekNumFromDate(20180201) = 1, CONST);     // Start of week = Sunday
    ASSERT(Date.MonthWeekNumFromDate(20180304, 2) = 1, CONST);  // Start of week = Monday
    ASSERT(Date.MonthWeekNumFromDate(20180305, 2) = 2, CONST);  // Start of week = Monday
    ASSERT(Date.MonthWeekNumFromDate(20180305, 3) = 1, CONST);  // Start of week = Tuesday
    ASSERT(Date.MonthWeekNumFromDate(20180306, 3) = 2, CONST);  // Start of week = Tuesday
    ASSERT(Date.MonthWeekNumFromDate(20180306, 4) = 1, CONST);  // Start of week = Wednesday
    ASSERT(Date.MonthWeekNumFromDate(20180307, 4) = 2, CONST);  // Start of week = Wednesday
    ASSERT(Date.MonthWeekNumFromDate(20180307, 5) = 1, CONST);  // Start of week = Thursday
    ASSERT(Date.MonthWeekNumFromDate(20180308, 5) = 2, CONST);  // Start of week = Thursday
    ASSERT(Date.MonthWeekNumFromDate(20180301, 6) = 1, CONST);  // Start of week = Friday
    ASSERT(Date.MonthWeekNumFromDate(20180302, 6) = 2, CONST);  // Start of week = Friday
    ASSERT(Date.MonthWeekNumFromDate(20180302, 7) = 1, CONST);  // Start of week = Saturday
    ASSERT(Date.MonthWeekNumFromDate(20180303, 7) = 2, CONST);  // Start of week = Saturday

    ASSERT(EXISTS(Date.TimeZone.TZ_Data), CONST);

    ASSERT(TRUE, CONST)
  ];

  EXPORT TestDynamicFunctions := [
    ASSERT(Date.SecondsFromParts(1999,2,1,12,34,56,FALSE) = 917872496);     // UTC
    ASSERT(Date.SecondsFromParts(1965,2,17,0,0,0,FALSE) = -153705600);      // UTC

    // UTC vs. local round-trip testing
    ASSERT(ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,FALSE),FALSE)).year = ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,TRUE),TRUE)).year);
    ASSERT(ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,FALSE),FALSE)).month = ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,TRUE),TRUE)).month);
    ASSERT(ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,FALSE),FALSE)).day = ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,TRUE),TRUE)).day);
    ASSERT(ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,FALSE),FALSE)).hour = ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,TRUE),TRUE)).hour);
    ASSERT(ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,FALSE),FALSE)).minute = ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,TRUE),TRUE)).minute);
    ASSERT(ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,FALSE),FALSE)).second = ROW(Std.Date.CreateDateTimeFromSeconds(Std.Date.SecondsFromParts(1999,2,1,12,34,56,TRUE),TRUE)).second);

    ASSERT(Date.SecondsToParts(917872496).year = 1999);
    ASSERT(Date.SecondsToParts(917872496).month = 2);
    ASSERT(Date.SecondsToParts(917872496).day = 1);
    ASSERT(Date.SecondsToParts(917872496).hour = 12);
    ASSERT(Date.SecondsToParts(917872496).minute = 34);
    ASSERT(Date.SecondsToParts(917872496).second = 56);

    ASSERT(Date.SecondsToParts(-153705600).year = 1965);
    ASSERT(Date.SecondsToParts(-153705600).month = 2);
    ASSERT(Date.SecondsToParts(-153705600).day = 17);
    ASSERT(Date.SecondsToParts(-153705600).hour = 0);
    ASSERT(Date.SecondsToParts(-153705600).minute = 0);
    ASSERT(Date.SecondsToParts(-153705600).second = 0);

    ASSERT(Date.SecondsToParts(0).year = 1970);     // Epoch test
    ASSERT(Date.SecondsToParts(0).month = 1);       // Epoch test
    ASSERT(Date.SecondsToParts(0).day = 1);         // Epoch test
    ASSERT(Date.SecondsToParts(0).hour = 0);        // Epoch test
    ASSERT(Date.SecondsToParts(0).minute = 0);      // Epoch test
    ASSERT(Date.SecondsToParts(0).second = 0);      // Epoch test

    ASSERT(Date.DayOfYear(20000101) = 1);
    ASSERT(Date.DayOfYear(20001231) = 366);
    ASSERT(Date.DayOfYear(20011231) = 365);

    ASSERT(Date.DayOfWeek(20140130) = 5);   // 5=Thursday
    ASSERT(Date.DayOfWeek(19650217) = 4);   // 4=Wednesday
    ASSERT(Date.DayOfWeek(20530213) = 5);   // 5=Thursday

    ASSERT(Date.AdjustDate(20000130, month_delta:=1) = 20000301);
    ASSERT(Date.AdjustDate(20000130, month_delta:=1, day_delta:=-1) = 20000229);
    ASSERT(Date.AdjustDate(20000229, year_delta:=1) = 20010301);
    ASSERT(Date.AdjustDate(20000229, year_delta:=-1) = 19990301);
    ASSERT(Date.AdjustDate(19650217, year_delta:=49) = 20140217);

    ASSERT(Date.AdjustDateBySeconds(20140130, 172800) = 20140201);

    ASSERT(Date.AdjustTime(180000, hour_delta:=7) = 010000);
    ASSERT(Date.AdjustTime(180000, minute_delta:=420) = 010000);
    ASSERT(Date.AdjustTime(180000, second_delta:=-86400) = 180000);

    ASSERT(Date.AdjustTimeBySeconds(180000, 86400) = 180000);
    ASSERT(Date.AdjustTimeBySeconds(180000, -86400) = 180000);

    ASSERT(Date.AdjustSeconds(917872496, hour_delta:=1) = 917876096);

    ASSERT(Date.AdjustCalendar(20140130, month_delta:=1) = 20140228);
    ASSERT(Date.AdjustCalendar(20000229, year_delta:=1) = 20010228);
    ASSERT(Date.AdjustCalendar(20000229, year_delta:=4) = 20040229);

    ASSERT(vSeconds + vLocalTimeZoneOffset BETWEEN vSecondsLocal-1 AND vSecondsLocal+1);

    ASSERT(vTimestamp != vIndTimestamp); // Test for non-pure calls to C++ code

    ASSERT(Date.TimestampToSeconds(vTimestamp) BETWEEN vSeconds-1 AND vSeconds+1);
    ASSERT(Date.TimestampToSeconds(vTimestampLocal) BETWEEN vSecondsLocal-1 AND vSecondsLocal+1);

    // IsLocalDaylightSavingsInEffect() -- not possible to check without pinning both cluster location and date

    ASSERT(Date.DatesForMonth(20141215).startDate = 20141201);
    ASSERT(Date.DatesForMonth(20141215).endDate = 20141231);

    ASSERT(Date.DatesForWeek(20141030).startDate = 20141026);
    ASSERT(Date.DatesForWeek(20141030).endDate = 20141101);

    ASSERT(Date.IsValidDate(vDate, 2014, 2050));
    ASSERT(NOT Date.IsValidDate(vDate, 2000, 2010));
    ASSERT(Date.IsValidDate(vToday, 2014, 2050));
    ASSERT(NOT Date.IsValidDate(vToday, 2000, 2010));
    ASSERT(Date.IsValidDate(20141030) = TRUE);
    ASSERT(Date.IsValidDate(20000229) = TRUE);
    ASSERT(Date.IsValidDate(20010229) = FALSE); // Invalid date (leap year check)
    ASSERT(Date.IsValidDate(20141000) = FALSE); // Invalid day
    ASSERT(Date.IsValidDate(20141032) = FALSE); // Invalid day
    ASSERT(Date.IsValidDate(20000001) = FALSE); // Invalid month
    ASSERT(Date.IsValidDate(20001301) = FALSE); // Invalid month

    ASSERT(Date.IsValidTime(vTime));
    ASSERT(Date.IsValidTime(vTimeLocal));
    ASSERT(Date.IsValidTime(123456) = TRUE);
    ASSERT(Date.IsValidTime(123465) = FALSE); // Invalid seconds
    ASSERT(Date.IsValidTime(127456) = FALSE); // Invalid minutes
    ASSERT(Date.IsValidTime(243456) = FALSE); // Invalid hours

    ASSERT(NOT Date.IsValidGregorianDate(16001231));
    ASSERT(Date.IsValidGregorianDate(16010101));
    ASSERT(Date.IsValidGregorianDate(308271231));
    ASSERT(NOT Date.IsValidGregorianDate(308280101));

    ASSERT(vCreateDateTimeFromSeconds.year = 1999);
    ASSERT(vCreateDateTimeFromSeconds.month = 2);
    ASSERT(vCreateDateTimeFromSeconds.day = 1);
    ASSERT(vCreateDateTimeFromSeconds.hour = 12);
    ASSERT(vCreateDateTimeFromSeconds.minute = 34);
    ASSERT(vCreateDateTimeFromSeconds.second = 56);

    ASSERT(vCreateDateFromSeconds.year = 1999);
    ASSERT(vCreateDateFromSeconds.month = 2);
    ASSERT(vCreateDateFromSeconds.day = 1);

    ASSERT(vCreateTimeFromSeconds.hour = 12);
    ASSERT(vCreateTimeFromSeconds.minute = 34);
    ASSERT(vCreateTimeFromSeconds.second = 56);

    ASSERT(vCreateTime.hour = 12);
    ASSERT(vCreateTime.minute = 34);
    ASSERT(vCreateTime.second = 56);

    ASSERT(vCreateDateTime.year = 1999);
    ASSERT(vCreateDateTime.month = 2);
    ASSERT(vCreateDateTime.day = 1);
    ASSERT(vCreateDateTime.hour = 12);
    ASSERT(vCreateDateTime.minute = 34);
    ASSERT(vCreateDateTime.second = 56);

    ASSERT(Date.TimeFromTimeRec(vCreateTime) = 123456);

    ASSERT(Date.DateFromDateTimeRec(vCreateDateTime) = 19990201);

    ASSERT(Date.TimeFromDateTimeRec(vCreateDateTime) = 123456);

    ASSERT(Date.SecondsFromDateTimeRec(vCreateDateTime) = 917872496);

    ASSERT(COUNT(Date.TimeZone.UniqueTZAbbreviations()) = 227);
    ASSERT(COUNT(Date.TimeZone.UniqueTZLocations()) = 15);
    ASSERT(COUNT(Date.TimeZone.TZDataForLocation('NORTH AMERICA')) = 28);
    // SecondsBetweenTZ for ambiguous abbreviations using first location found
    ASSERT(Date.TimeZone.SecondsBetweenTZ('EST', 'CST') = -3600);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST') = 41400);
    // SecondsBetweenTZ for ambiguous abbreviations using explicit locations
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', fromLocation := 'NORTH AMERICA') = 41400);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', fromLocation := 'CENTRAL AMERICA') = 41400);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', fromLocation := 'ASIA') = -9000);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', fromLocation := 'CARIBBEAN') = 37800);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', toLocation := 'ASIA') = 41400);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', toLocation := 'EUROPE') = 25200);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', toLocation := 'ISRAEL') = 28800);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', fromLocation := 'NORTH AMERICA', toLocation := 'ASIA') = 41400);
    ASSERT(Date.TimeZone.SecondsBetweenTZ('CST', 'IST', fromLocation := 'ASIA', toLocation := 'ISRAEL') = -21600);
    // AdjustTimeTZ for ambiguous abbreviations using first location found
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'EST', 'CST') = 110000);
    // AdjustTimeTZ for ambiguous abbreviations using explicit locations
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', fromLocation := 'NORTH AMERICA') = 233000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', fromLocation := 'CENTRAL AMERICA') = 233000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', fromLocation := 'ASIA') = 93000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', fromLocation := 'CARIBBEAN') = 223000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', toLocation := 'ASIA') = 233000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', toLocation := 'EUROPE') = 190000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', toLocation := 'ISRAEL') = 200000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', fromLocation := 'NORTH AMERICA', toLocation := 'ASIA') = 233000);
    ASSERT(Date.TimeZone.AdjustTimeTZ(120000, 'CST', 'IST', fromLocation := 'ASIA', toLocation := 'ISRAEL') = 60000);
    // ToLocalTime for ambiguous abbreviations using first location found
    ASSERT(Date.TimeZone.ToLocalTime(120000, 'IST') = 173000);
    // ToLocalTime for ambiguous abbreviations using explicit locations
    ASSERT(Date.TimeZone.ToLocalTime(120000, 'IST', toLocation := 'ASIA') = 173000);
    ASSERT(Date.TimeZone.ToLocalTime(120000, 'IST', toLocation := 'EUROPE') = 130000);
    ASSERT(Date.TimeZone.ToLocalTime(120000, 'IST', toLocation := 'ISRAEL') = 140000);
    // ToUTCTime for ambiguous abbreviations using first location found
    ASSERT(Date.TimeZone.ToUTCTime(120000, 'IST') = 63000);
    // ToUTCTime for ambiguous abbreviations using explicit locations
    ASSERT(Date.TimeZone.ToUTCTime(120000, 'IST', fromLocation := 'ASIA') = 63000);
    ASSERT(Date.TimeZone.ToUTCTime(120000, 'IST', fromLocation := 'EUROPE') = 110000);
    ASSERT(Date.TimeZone.ToUTCTime(120000, 'IST', fromLocation := 'ISRAEL') = 100000);

    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_utc)[1].seconds_to_utc_is_valid = TRUE);
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_utc)[1].seconds_to_utc = 21600);
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_utc)[1].seconds_to_utc_tz = 'UTC');
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_utc)[1].seconds_to_utc_location = 'WORLDWIDE');
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_pt, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].seconds_to_pt_is_valid = TRUE);
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_pt, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].seconds_to_pt = -7200);
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_pt, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].seconds_to_pt_tz = 'PT');
    ASSERT(Date.TimeZone.AppendTZOffset(vTZInputSample, tz, seconds_to_pt, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].seconds_to_pt_location = 'NORTH AMERICA');
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, utc_time)[1].utc_time_is_valid = TRUE);
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, utc_time)[1].utc_time = 180000);
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, utc_time)[1].utc_time_tz = 'UTC');
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, utc_time)[1].utc_time_location = 'WORLDWIDE');
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, pt_time, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].pt_time_is_valid = TRUE);
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, pt_time, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].pt_time = 100000);
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, pt_time, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].pt_time_tz = 'PT');
    ASSERT(Date.TimeZone.AppendTZAdjustedTime(vTZInputSample, time, tz, pt_time, toTimeZoneAbbrev := 'PT', toLocation := 'NORTH AMERICA')[1].pt_time_location = 'NORTH AMERICA');

    ASSERT(TRUE)
  ];

  EXPORT TestDynamic := MODULE
    //Iterate through all lots of dates, incrementing the day and the date to check they convert correctly.
    Date_rec := Date.Date_rec;
    test_rec := { Date.Days_t day, Date_rec gregorian, Date_rec julian };
    firstDate := DATASET([{ 1, ROW(Date.CreateDate(1,1,1)), ROW(Date.CreateDate(1,1,1))}], test_rec);
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
       result1 := ASSERT(next, Date.ToGregorianDate(next.day) = Date.DateFromDateRec(next.gregorian));
       result2 := ASSERT(result1, next.day = Date.FromGregorianDate(Date.DateFromDateRec(next.gregorian)));
       result3 := ASSERT(result2, Date.ToJulianDate(next.day) = Date.DateFromDateRec(next.julian));
       result4 := ASSERT(result3, next.day = Date.FromJulianDate(Date.DateFromDateRec(next.julian)));
       RETURN result4;
    END;

    x := LOOP(firstDate, 1000000, processNextDate(ROWS(LEFT)));

    EXPORT Test01 := OUTPUT(x);
  END;

  EXPORT Main := [EVALUATE(TestConstant), EVALUATE(TestDynamicFunctions), EVALUATE(TestDynamic)];

END;
