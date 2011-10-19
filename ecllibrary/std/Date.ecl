/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

EXPORT Date := MODULE

// Three different date representations are defined
// A record stucture with the different elements separated out.
EXPORT Date_rec := RECORD
    INTEGER2   year;
    UNSIGNED1   month;
    UNSIGNED1   day;
END;

//An unsigned number holding a date in the decimal form YYYYMMDD.  This type doesn't support dates before 1AD.
EXPORT Date_t := UNSIGNED4;

//A number of elapsed days.  Origin depends on the function called.
EXPORT Days_t := UNSIGNED4;

/*
 * Extracts the year from a date type.
 *
 * @param date          The date.
 * @return              An integer representing the year.
 */

EXPORT Year(Date_t date) := date DIV 10000;

/*
 * Extracts the month from a date type.
 *
 * @param date          The date.
 * @return              An integer representing the year.
 */

EXPORT Month(Date_t date) := (date DIV 100) % 100;

/*
 * Extracts the day of the month from a date type.
 *
 * @param date          The date.
 * @return              An integer representing the year.
 */

EXPORT Day(Date_t date) := date % 100;

/*
 * Combines year, month day to create a date type.
 *
 * @param year          The year (0-9999).
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              A date created by combining the fields.
 */

EXPORT DateFromParts(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := ( year * 100 + month ) * 100 + day;

/*
 * Tests whether the year is a leap year in the Gregorian calendar (Or proleptic Gregorian).
 *
 * @param year          The year (0-9999).
 * @return              True if the year is a leap year.
 */

EXPORT IsLeapYear(INTEGER2 year) := (year % 4 = 0) AND ((year % 100 != 0) OR (year % 400 = 0));

SHARED YearDelta := +4800;          // Offset the years by 4800 so dates up to -4713 work

SHARED GregorianDateOrigin := -1753469;      // 1 Jan 1AD = 1

/*
 * Combines year, month, day in the Gregorian calendar to create the number days since 31st December 1BC.
 *
 * @param year          The year (-4713..9999).
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT FromGregorianYMD(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := FUNCTION
    //See Frequently Asked Questions about Calendars by Claus Toendering
    a := (14 - month) DIV 12;
    y := year + YearDelta - a;
    m := month + 12*a - 3;
    jd := day + (153 * m + 2) DIV 5 + 365 * y + y DIV 4 - y DIV 100 + y DIV 400;
    RETURN jd + (GregorianDateOrigin-1);
END;

/*
 * Converts the number days since 31st December 1BC to a date in the Gregorian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              Module containing Year, Month, Day in the Gregorian calendar
 */

EXPORT ToGregorianYMD(Days_t days) := FUNCTION
    //See Fliegel and van Flandern (1968) and other quoted sources (e.g., http://www.ortelius.de/kalender/calc_en.php)
    //Process as 4, 100 and 400 year cycles.
    daysIn4Years := 3*365+366;
    daysIn100Years := 25*daysIn4Years-1;
    daysIn400Years := 4*daysIn100Years+1;

    //Calulate days in each of the cycles.
    adjustedDays := days - GregorianDateOrigin;
    num400Years := adjustedDays div daysIn400Years;
    rem400Years := adjustedDays % daysIn400Years;

    num100Years := ((rem400Years div daysIn100Years + 1) * 3) DIV 4;
    rem100Years := rem400Years - num100Years * daysIn100Years;

    num4Years := rem100Years div daysIn4Years;
    rem4Years := rem100Years % daysIn4Years;

    years := ((rem4Years div 365 + 1) * 3) DIV 4;
    numdays := rem4Years - years * 365;

    //Now calculate the actual year, month day
    y := num400Years * 400 + num100Years * 100 + num4Years * 4 + years;
    m := (numdays * 5 + 308) div 153 - 2;
    d := numdays - (m + 4) * 153 div 5 + 122;
    result := MODULE
        EXPORT year := (y + (m + 2) div 12) - YearDelta;
        EXPORT month := (m + 2) % 12 + 1;
        EXPORT day := d + 1;
    END;
    return result;
END;

/*
 * Converts a date in the Gregorian calendar to the number days since 31st December 1BC.
 *
 * @param date          The date (using the Gregorian calendar)
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT FromGregorianDate(Date_t date) := DEFINE FromGregorianYMD(Year(date), Month(date), Day(date));

/*
 * Converts the number days since 31st December 1BC to a date in the Gregorian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              A date_t in the Gregorian calendar
 */

EXPORT ToGregorianDate(Days_t days) := DEFINE FUNCTION
    date := ToGregorianYMD(days);
    RETURN DateFromParts(date.year, date.month, date.day);
END;

day_of_year(INTEGER2 month, UNSIGNED1 day) := CHOOSE( month+1,0,0,31,59,90,120,151,181,212,243,273,304,334 ) + day;

/*
 * Extracts the day of the year from a date in the Gregorian calendar.
 *
 * @param date          The date
 * @return              The day of the year (1..366)
 */

EXPORT DayOfYear(Date_t d) := day_of_year(Month(d),Day(d)) + IF(IsLeapYear(Year(d)) AND Month(d) > 2, 1, 0);

SHARED JulianDateOrigin := -1753505;                // 1 Jan 1AD = 1

/*
 * Tests whether the year is a leap year in the Julian calendar.
 *
 * @param year          The year (0-9999).
 * @return              True if the year is a leap year.
 */

EXPORT IsJulianLeapYear(INTEGER2 year) := (year % 4 = 0);

/*
 * Combines year, month, day in the Julian calendar to create the number days since 31st December 1BC.
 *
 * @param year          The year (-4800..9999).
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT FromJulianYMD(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := FUNCTION
    //See Frequently Asked Questions about Calendars by Claus Toendering
    a := (14 - month) DIV 12;
    y := year + YearDelta - a;
    m := month + 12*a - 3;
    jd := day + (153 * m + 2) DIV 5 + 365 * y + y DIV 4;
    RETURN jd + (JulianDateOrigin-1);
END;

/*
 * Converts the number days since 31st December 1BC to a date in the Julian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              Module containing Year, Month, Day in the Julian calendar
 */

EXPORT ToJulianYMD(Days_t days) := FUNCTION
    //See Frequently Asked Questions about Calendars by Claus Toendering
    daysIn4Years := 3*365+366;
    c := days - JulianDateOrigin;
    d := (4 * c + 3) DIV daysIn4Years;
    e := c - ((daysIn4Years * d) DIV 4);
    m := (5 * e + 2) DIV 153;
    result := MODULE
        EXPORT day := e - ((153 * m + 2) DIV 5) + 1;
        EXPORT month := m + 3 - 12 * (m DIV 10);
        EXPORT year := d - YearDelta + (m DIV 10);
    END;
    RETURN result;
END;

/*
 * Converts a date in the Julian calendar to the number days since 31st December 1BC.
 *
 * @param date          The date (using the Julian calendar)
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT FromJulianDate(Date_t date) := DEFINE FromJulianYMD(Year(date), Month(date), Day(date));

/*
 * Converts the number days since 31st December 1BC to a date in the Julian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              A date_t in the Julian calendar
 */

EXPORT ToJulianDate(Days_t days) := DEFINE FUNCTION
    date := ToJulianYMD(days);
    RETURN DateFromParts(date.year, date.month, date.day);
END;

SHARED Date1900Delta := 693596;      // 1 Jan 1900 = 0

/*
 * Returns the number of days since 1st January 1900 (using the Gregorian Calendar)
 *
 * @param year          The year (-4713..9999).
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              The number of elapsed days since 1st January 1900
 */

EXPORT DaysSince1900(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := FromGregorianYMD(year, month, day) - Date1900Delta;

/*
 * Returns the number of days since 1st January 1900 (using the Gregorian Calendar)
 *
 * @param date          The date
 * @return              The number of elapsed days since 1st January 1900
 */

EXPORT ToDaysSince1900(Date_t date) := DaysSince1900(Year(date),Month(date),Day(date));

/*
 * Converts the number days since 1st January 1900 to a date in the Julian calendar.
 *
 * @param days          The number of elapsed days since 1st Jan 1900
 * @return              A date_t in the Julian calendar
 */

EXPORT FromDaysSince1900(Days_t days) := ToGregorianDate(days + Date1900Delta);

/*
 * Returns the current date
 *
 * @return              A date_t representing the current date.
 */
import lib_stringlib.StringLib;
EXPORT Today() := (date_t)StringLib.GetDateYYYYMMDD();

/*
 * A transform to create a date_rec from the individual elements
 *
 * @param year          The year
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              A transform that creates a date_rec containing the date.
 */
EXPORT Date_rec CreateDate(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := TRANSFORM
    SELF.year := year;
    SELF.month := month;
    SELF.day := day;
END;

/*
 * Combines the fields from a date_rec to create a date_t
 *
 * @param date          The row containing the date.
 * @return              A date_t representing the combined values.
 */
EXPORT DateFromRec(date_rec date) := ( date.year * 100 + date.month ) * 100 + date.day;

END;
