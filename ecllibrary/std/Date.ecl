/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

/* The functions defined in this module are provisional, and subject to change */

IMPORT lib_stringlib.StringLib;
IMPORT lib_timelib.TimeLib;

EXPORT Date := MODULE


// A record structure with the different date elements separated out.
EXPORT Date_rec := RECORD
    INTEGER2    year;
    UNSIGNED1   month;
    UNSIGNED1   day;
END;


// An unsigned number holding a date in the decimal form YYYYMMDD.
// This type doesn't support dates before 1AD.
EXPORT Date_t := UNSIGNED4;


// A number of elapsed days.  Origin depends on the function called.
EXPORT Days_t := INTEGER4;


// A record structure with the different time elements separated out.
EXPORT Time_rec := RECORD
    UNSIGNED1   hour;
    UNSIGNED1   minute;
    UNSIGNED1   second;
END;


// An unsigned number holding a time of day in the decimal form HHMMDD.
EXPORT Time_t := UNSIGNED3;


// A signed number holding a number of seconds.  Can be used to represent either
// a duration or the number of seconds since epoch (Jan 1, 1970).
EXPORT Seconds_t := INTEGER8;


// A record structure with the different date and time elements separated out.
EXPORT DateTime_rec := RECORD
    Date_rec;
    Time_Rec;
END;


// A signed number holding a number of microseconds.  Can be used to represent
// either a duration or the number of microseconds since epoch (Jan 1, 1970).
EXPORT Timestamp_t := INTEGER8;


/**
 * Extracts the year from a date type.
 *
 * @param date          The date.
 * @return              An integer representing the year.
 */

EXPORT INTEGER2 Year(Date_t date) := date DIV 10000;


/**
 * Extracts the month from a date type.
 *
 * @param date          The date.
 * @return              An integer representing the year.
 */

EXPORT UNSIGNED1 Month(Date_t date) := (date DIV 100) % 100;


/**
 * Extracts the day of the month from a date type.
 *
 * @param date          The date.
 * @return              An integer representing the year.
 */

EXPORT UNSIGNED1 Day(Date_t date) := date % 100;


/**
 * Extracts the hour from a time type.
 *
 * @param time          The time.
 * @return              An integer representing the hour.
 */

EXPORT UNSIGNED1 Hour(Time_t time) := time DIV 10000;


/**
 * Extracts the minutes from a time type.
 *
 * @param time          The time.
 * @return              An integer representing the minutes.
 */

EXPORT UNSIGNED1 Minute(Time_t time) := (time DIV 100) % 100;


/**
 * Extracts the seconds from a time type.
 *
 * @param time          The time.
 * @return              An integer representing the seconds.
 */

EXPORT UNSIGNED1 Second(Time_t time) := time % 100;


/**
 * Combines year, month day to create a date type.
 *
 * @param year          The year (0-9999).
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              A date created by combining the fields.
 */

EXPORT Date_t DateFromParts(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := (year * 100 + month) * 100 + day;


/**
 * Combines hour, minute second to create a time type.
 *
 * @param hour          The hour (0-23).
 * @param minute        The minute (0-59).
 * @param second        The second (0-59).
 * @return              A time created by combining the fields.
 */

EXPORT Time_t TimeFromParts(UNSIGNED1 hour, UNSIGNED1 minute, UNSIGNED1 second) := (hour * 100 + minute) * 100 + second;


/**
 * Combines date and time components to create a seconds type.  The date must
 * be represented within the Gregorian calendar after the year 1600.
 *
 * @param year                  The year (1601-30827).
 * @param month                 The month (1-12).
 * @param day                   The day (1..daysInMonth).
 * @param hour                  The hour (0-23).
 * @param minute                The minute (0-59).
 * @param second                The second (0-59).
 * @param is_local_time         TRUE if the datetime components are expressed
 *                              in local time rather than UTC, FALSE if the
 *                              components are expressed in UTC.  Optional,
 *                              defaults to FALSE.
 * @return                      A Seconds_t value created by combining the fields.
 */

EXPORT Seconds_t SecondsFromParts(INTEGER2 year,
                                  UNSIGNED1 month,
                                  UNSIGNED1 day,
                                  UNSIGNED1 hour,
                                  UNSIGNED1 minute,
                                  UNSIGNED1 second,
                                  BOOLEAN is_local_time = FALSE) :=
    TimeLib.SecondsFromParts(year, month, day, hour, minute, second, is_local_time);


/**
 * Converts the number of seconds since epoch to a structure containing
 * date and time parts.  The result must be representable within the
 * Gregorian calendar after the year 1600.
 *
 * @param seconds               The number of seconds since epoch.
 * @param is_local_time         TRUE if seconds is expressed in local time
 *                              rather than UTC, FALSE if seconds is expressed
 *                              in UTC.  Optional, defaults to FALSE.
 * @return                      Module with exported attributes for year, month,
 *                              day, hour, minute, second, day_of_week, date
 *                              and time.
 */

EXPORT SecondsToParts(Seconds_t seconds, BOOLEAN is_local_time = FALSE) := FUNCTION
    parts := ROW(TimeLib.SecondsToParts(seconds, is_local_time));

    result := MODULE
        EXPORT INTEGER2 year := parts.year + 1900;
        EXPORT UNSIGNED1 month := parts.mon + 1;
        EXPORT UNSIGNED1 day := parts.mday;
        EXPORT UNSIGNED1 hour := parts.hour;
        EXPORT UNSIGNED1 minute := parts.min;
        EXPORT UNSIGNED1 second := parts.sec;
        EXPORT UNSIGNED1 day_of_week := parts.wday + 1;
        EXPORT Date_t date := DateFromParts(year,month,day);
        EXPORT Time_t time := TimeFromParts(hour,minute,second);
    END;

    RETURN result;
END;


/**
 * Converts the number of microseconds since epoch to the number of seconds
 * since epoch.
 *
 * @param timestamp             The number of microseconds since epoch.
 * @return                      The number of seconds since epoch.
 */

EXPORT Seconds_t TimestampToSeconds(Timestamp_t timestamp) := timestamp DIV 1000000;

/**
 * Tests whether the year is a leap year in the Gregorian calendar.
 *
 * @param year          The year (0-9999).
 * @return              True if the year is a leap year.
 */

EXPORT BOOLEAN IsLeapYear(INTEGER2 year) := (year % 4 = 0) AND ((year % 100 != 0) OR (year % 400 = 0));


/**
 * Tests whether a date is a leap year in the Gregorian calendar.
 *
 * @param date          The date.
 * @return              True if the year is a leap year.
 */

EXPORT BOOLEAN IsDateLeapYear(Date_t date) := IsLeapYear(Year(date));

SHARED YearDelta := +4800;  // Offset the years by 4800 so dates up to -4713 work

SHARED GregorianDateOrigin := -1753469;      // 1 Jan 1AD = 1


/**
 * Combines year, month, day in the Gregorian calendar to create the number
 * days since 31st December 1BC.
 *
 * @param year          The year (-4713..9999).
 * @param month         The month (1-12).  A missing value (0) is treated as 1.
 * @param day           The day (1..daysInMonth).  A missing value (0) is treated as 1.
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT Days_t FromGregorianYMD(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := FUNCTION
    //See Frequently Asked Questions about Calendars by Claus Toendering
    safeDay := MAX(1, day); // treat 0 as 1
    safeMonth := MAX(1, month); // treat 0 as 1
    a := (14 - safeMonth) DIV 12;
    y := year + YearDelta - a;
    m := safeMonth + 12*a - 3;
    jd := safeDay + (153 * m + 2) DIV 5 + 365 * y + y DIV 4 - y DIV 100 + y DIV 400;

    RETURN jd + (GregorianDateOrigin - 1);
END;


/**
 * Converts the number days since 31st December 1BC to a date in the Gregorian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              Module containing Year, Month, Day in the Gregorian calendar
 */

EXPORT ToGregorianYMD(Days_t days) := FUNCTION
    // See Fliegel and van Flandern (1968) and other quoted sources
    // (e.g., http://www.ortelius.de/kalender/calc_en.php)
    // Process as 4, 100 and 400 year cycles.
    daysIn4Years := 3*365+366;
    daysIn100Years := 25*daysIn4Years-1;
    daysIn400Years := 4*daysIn100Years+1;

    // Calculate days in each of the cycles.
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

    RETURN result;
END;


/**
 * Converts a date in the Gregorian calendar to the number days since 31st December 1BC.
 *
 * @param date          The date (using the Gregorian calendar)
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT Days_t FromGregorianDate(Date_t date) :=
    DEFINE FromGregorianYMD(Year(date), Month(date), Day(date));


/**
 * Converts the number days since 31st December 1BC to a date in the Gregorian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              A Date_t in the Gregorian calendar
 */

EXPORT Date_t ToGregorianDate(Days_t days) := DEFINE FUNCTION
    date := ToGregorianYMD(days);

    RETURN DateFromParts(date.year, date.month, date.day);
END;


/**
 * Returns a number representing the day of the year indicated by the given date.
 * The date must be in the Gregorian calendar after the year 1600.
 *
 * @param date          A Date_t value.
 * @return              A number (1-366) representing the number of days since
 *                      the beginning of the year.
 */

EXPORT UNSIGNED2 DayOfYear(Date_t date) := FUNCTION
    theYear := Year(date);
    theMonth := Month(date);
    theDay := Day(date);

    dayNum := TimeLib.GetDayOfYear(theYear, theMonth, theDay) + 1;

    RETURN dayNum;
END;


/**
 * Returns a number representing the day of the week indicated by the given date.
 * The date must be in the Gregorian calendar after the year 1600.
 *
 * @param date          A Date_t value.
 * @return              A number 1-7 representing the day of the week, where 1 = Sunday.
 */

EXPORT UNSIGNED1 DayOfWeek(Date_t date) := FUNCTION
    theYear := Year(date);
    theMonth := Month(date);
    theDay := Day(date);

    dayCode := TimeLib.GetDayOfWeek(theYear, theMonth, theDay) + 1;

    RETURN dayCode;
END;


/**
 * Tests whether the year is a leap year in the Julian calendar.
 *
 * @param year          The year (0-9999).
 * @return              True if the year is a leap year.
 */

EXPORT BOOLEAN IsJulianLeapYear(INTEGER2 year) := (year % 4 = 0);


SHARED JulianDateOrigin := -1753505;                // 1 Jan 1AD = 1


/**
 * Combines year, month, day in the Julian calendar to create the number
 * days since 31st December 1BC.
 *
 * @param year          The year (-4800..9999).
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT Days_t FromJulianYMD(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := FUNCTION
    //See Frequently Asked Questions about Calendars by Claus Toendering
    a := (14 - month) DIV 12;
    y := year + YearDelta - a;
    m := month + 12*a - 3;
    jd := day + (153 * m + 2) DIV 5 + 365 * y + y DIV 4;

    RETURN jd + (JulianDateOrigin-1);
END;


/**
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
        EXPORT UNSIGNED1 day := e - ((153 * m + 2) DIV 5) + 1;
        EXPORT UNSIGNED1 month := m + 3 - 12 * (m DIV 10);
        EXPORT INTEGER2 year := d - YearDelta + (m DIV 10);
    END;

    RETURN result;
END;


/**
 * Converts a date in the Julian calendar to the number days since 31st December 1BC.
 *
 * @param date          The date (using the Julian calendar)
 * @return              The number of elapsed days (1 Jan 1AD = 1)
 */

EXPORT Days_t FromJulianDate(Date_t date) := DEFINE FromJulianYMD(Year(date), Month(date), Day(date));


/**
 * Converts the number days since 31st December 1BC to a date in the Julian calendar.
 *
 * @param days          The number of elapsed days (1 Jan 1AD = 1)
 * @return              A Date_t in the Julian calendar
 */

EXPORT Date_t ToJulianDate(Days_t days) := DEFINE FUNCTION
    date := ToJulianYMD(days);

    RETURN DateFromParts(date.year, date.month, date.day);
END;

SHARED Date1900Delta := 693596;      // 1 Jan 1900 = 0


/**
 * Returns the number of days since 1st January 1900 (using the Gregorian Calendar)
 *
 * @param year          The year (-4713..9999).
 * @param month         The month (1-12).  A missing value (0) is treated as 1.
 * @param day           The day (1..daysInMonth).  A missing value (0) is treated as 1.
 * @return              The number of elapsed days since 1st January 1900
 */

EXPORT Days_t DaysSince1900(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) :=
    FromGregorianYMD(year, month, day) - Date1900Delta;


/**
 * Returns the number of days since 1st January 1900 (using the Gregorian Calendar)
 *
 * @param date          The date
 * @return              The number of elapsed days since 1st January 1900
 */

EXPORT Days_t ToDaysSince1900(Date_t date) := DaysSince1900(Year(date),Month(date),Day(date));


/**
 * Converts the number days since 1st January 1900 to a date in the Julian calendar.
 *
 * @param days          The number of elapsed days since 1st Jan 1900
 * @return              A Date_t in the Julian calendar
 */

EXPORT Date_t FromDaysSince1900(Days_t days) := ToGregorianDate(days + Date1900Delta);


/**
 * Calculate the number of whole years between two dates.
 *
 * @param from          The first date
 * @param to            The last date
 * @return              The number of years between them.
 */

EXPORT INTEGER YearsBetween (Date_t from, Date_t to) := FUNCTION
    fromDate := MIN(from, to);
    toDate := MAX(from, to);
    years := Year(toDate) - Year(fromDate);
    adjustedYears := years - IF(Month(fromDate) > Month(toDate) OR (Month(fromDate) = Month(toDate) AND Day(fromDate) > Day(toDate)), 1, 0);

    RETURN adjustedYears * IF(from > to, -1, 1);
END;


/**
 * Calculate the number of whole months between two dates.
 *
 * If month_ends_equal is set to TRUE and the given dates fall on the last day
 * of their respective months, the dates' day values will be considered equal
 * regardless of their actual values.  For example, given the dates 20160331
 * and 20160930 (last day of March and last day of September, respectively), if
 * month_ends_equal is FALSE then the function's result will be 5; if
 * month_ends_equal is TRUE then the result will be 6.
 *
 * @param from              The first date
 * @param to                The last date
 * @param month_ends_equal  If TRUE and both dates fall on the last day of
 *                          their respective months, treat the difference
 *                          between the dates as whole months regardless of
 *                          the actual day values; if FALSE then the day value
 *                          of each date may be considered when calculating
 *                          the difference; OPTIONAL, defaults to FALSE
 * @return                  The number of months between them.
 */

EXPORT INTEGER MonthsBetween(Date_t from, Date_t to, BOOLEAN month_ends_equal = FALSE) := FUNCTION
    fromDate := MIN(from, to);
    toDate := MAX(from, to);
    years := Year(toDate) - Year(fromDate);
    months := Month(toDate) - Month(fromDate);
    result := years * 12 + months;
    fromIsMonthEnd := Day(fromDate) = CHOOSE(Month(fromDate),31,IF(IsLeapYear(Year(fromDate)),29,28),31,30,31,30,31,31,30,31,30,31);
    toIsMonthEnd := Day(toDate) = CHOOSE(Month(toDate),31,IF(IsLeapYear(Year(toDate)),29,28),31,30,31,30,31,31,30,31,30,31);
    adjustment := MAP
        (
            month_ends_equal AND fromIsMonthEnd AND toIsMonthEnd => 0,
            Day(fromDate) > Day(toDate) => 1,
            0
        );
    adjustedResult := result - adjustment;

    RETURN adjustedResult * IF(from > to, -1, 1);
END;


/**
 * Calculate the number of days between two dates.
 *
 * @param from          The first date
 * @param to            The last date
 * @return              The number of days between them.
 */

EXPORT INTEGER DaysBetween(Date_t from, Date_t to) := FUNCTION
    fromDays := FromGregorianDate(from);
    toDays   := FromGregorianDate(to);

    RETURN toDays - fromDays;
END;


/**
 * Combines the fields from a Date_rec to create a Date_t
 *
 * @param date          The row containing the date.
 * @return              A Date_t representing the combined values.
 */

EXPORT Date_t DateFromDateRec(Date_rec date) := (date.year * 100 + date.month) * 100 + date.day;


/**
 * Combines the fields from a Date_rec to create a Date_t
 *
 * @param date          The row containing the date.
 * @return              A Date_t representing the combined values.
 */

EXPORT Date_t DateFromRec(Date_rec date) := DateFromDateRec(date) : DEPRECATED('Replaced with DateFromDateRec() function');


/**
 * Combines the fields from a Time_rec to create a Time_t
 *
 * @param time          The row containing the time.
 * @return              A Time_t representing the combined values.
 */

EXPORT Time_t TimeFromTimeRec(Time_rec time) := (time.hour * 100 + time.minute) * 100 + time.second;


/**
 * Combines the date fields from a DateTime_rec to create a Date_t
 *
 * @param datetime      The row containing the datetime.
 * @return              A Date_t representing the combined values.
 */

EXPORT Date_t DateFromDateTimeRec(DateTime_rec datetime) := (datetime.year * 100 + datetime.month) * 100 + datetime.day;


/**
 * Combines the time fields from a DateTime_rec to create a Time_t
 *
 * @param datetime      The row containing the datetime.
 * @return              A Time_t representing the combined values.
 */

EXPORT Time_t TimeFromDateTimeRec(DateTime_rec datetime) := (datetime.hour * 100 + datetime.minute) * 100 + datetime.second;


/**
 * Combines the date and time fields from a DateTime_rec to create a Seconds_t
 *
 * @param datetime      The row containing the datetime.
 * @param is_local_time TRUE if the datetime components are expressed in local
 *                      time rather than UTC, FALSE if the components are
 *                      expressed in UTC.  Optional, defaults to FALSE.
 * @return              A Seconds_t representing the combined values.
 */

EXPORT Seconds_t SecondsFromDateTimeRec(DateTime_rec datetime, BOOLEAN is_local_time = FALSE) :=
    SecondsFromParts(datetime.year, datetime.month, datetime.day, datetime.hour, datetime.minute, datetime.second, is_local_time);


/**
 * Converts a string to a Date_t using the relevant string format.  The resulting
 * date must be representable within the Gregorian calendar after the year 1600.
 *
 * @param date_text     The string to be converted.
 * @param format        The format of the input string.
 *                      (See documentation for strftime)
 * @return              The date that was matched in the string.  Returns 0 if failed to match
 *                      or if the date components match but the result is an invalid date.
 *
 * Supported characters:
    %B          Full month name
    %b or %h    Abbreviated month name
    %d          Day of month (two digits)
    %e          Day of month (two digits, or a space followed by a single digit)
    %m          Month (two digits)
    %t          Whitespace
    %y          Year within century (00-99)
    %Y          Full year (yyyy)
    %j          Julian day (1-366)

Common date formats
    American    '%m/%d/%Y'  mm/dd/yyyy
    Euro        '%d/%m/%Y'  dd/mm/yyyy
    Iso format  '%Y-%m-%d'  yyyy-mm-dd
    Iso basic   '%Y%m%d'    yyyymmdd
                '%d-%b-%Y'  dd-mon-yyyy    e.g., '21-Mar-1954'
 */

EXPORT Date_t FromStringToDate(STRING date_text, VARSTRING format) :=
    StringLib.StringToDate(date_text, format);


/**
 * Converts a string to a date using the relevant string format.
 *
 * @param date_text     The string to be converted.
 * @param format        The format of the input string.
 *                      (See documentation for strftime)
 * @return              The date that was matched in the string.
 *                      Returns 0 if failed to match.
 */

EXPORT Date_t FromString(STRING date_text, VARSTRING format) :=
    FromStringToDate(date_text, format) : DEPRECATED('Replaced with FromStringToDate() function');


/**
 * Converts a string to a Time_t using the relevant string format.
 *
 * @param date_text     The string to be converted.
 * @param format        The format of the input string.
 *                      (See documentation for strftime)
 * @return              The time that was matched in the string.  Returns 0 if failed to match.
 *
 * Supported characters:
    %H          Hour (two digits)
    %k          Hour (two digits, or a space followed by a single digit)
    %M          Minute (two digits)
    %S          Second (two digits)
    %t          Whitespace
 */

EXPORT Time_t FromStringToTime(STRING time_text, VARSTRING format) :=
    StringLib.StringToTimeOfDay(time_text, format);


/**
 * Converts a string to a Seconds_t using the relevant string format.
 *
 * @param datetime_text The string to be converted.
 * @param format        The format of the input string.
 *                      (See documentation for strftime)
 * @param is_local_time TRUE if datetime_text is expressed in local time
 *                      rather than UTC, FALSE otherwise.  Optional, defaults
 *                      to FALSE.
 * @return              The seconds that was matched in the string.  Returns 0 if failed to match.
 *
 * Supported date characters:
    %B          Full month name
    %b or %h    Abbreviated month name
    %d          Day of month (two digits)
    %e          Day of month (two digits, or a space followed by a single digit)
    %m          Month (two digits)
    %y          Year within century (00-99)
    %Y          Full year (yyyy)
    %j          Julian day (1-366)
 *
 * Supported time characters:
    %H          Hour (two digits)
    %k          Hour (two digits, or a space followed by a single digit)
    %M          Minute (two digits)
    %S          Second (two digits)
 *
 * Other supported characters:
    %t          Whitespace
 */

EXPORT Seconds_t FromStringToSeconds(STRING datetime_text, VARSTRING format, BOOLEAN is_local_time = FALSE) :=
    TimeLib.StringToSeconds(datetime_text, format, is_local_time);


/**
 * Matches a string against a set of date string formats and returns a valid
 * Date_t object from the first format that successfully parses the string.
 *
 * @param date_text     The string to be converted.
 * @param formats       A set of formats to check against the string.
 *                      (See documentation for strftime)
 * @return              The date that was matched in the string.
 *                      Returns 0 if failed to match.
 *
 * Supported characters:
    %B          Full month name
    %b or %h    Abbreviated month name
    %d          Day of month (two digits)
    %e          Day of month (two digits, or a space followed by a single digit)
    %m          Month (two digits)
    %t          Whitespace
    %y          Year within century (00-99)
    %Y          Full year (yyyy)
    %j          Julian day (1-366)

Common date formats
    American    '%m/%d/%Y'  mm/dd/yyyy
    Euro        '%d/%m/%Y'  dd/mm/yyyy
    Iso format  '%Y-%m-%d'  yyyy-mm-dd
    Iso basic   '%Y%m%d'    yyyymmdd
                '%d-%b-%Y'  dd-mon-yyyy    e.g., '21-Mar-1954'
 */

EXPORT Date_t MatchDateString(STRING date_text, SET OF VARSTRING formats) :=
    StringLib.MatchDate(date_text, formats);


/**
 * Matches a string against a set of time string formats and returns a valid
 * Time_t object from the first format that successfully parses the string.
 *
 * @param time_text     The string to be converted.
 * @param formats       A set of formats to check against the string.
 *                      (See documentation for strftime)
 * @return              The time that was matched in the string.
 *                      Returns 0 if failed to match.
 */

EXPORT Time_t MatchTimeString(STRING time_text, SET OF VARSTRING formats) :=
    StringLib.MatchTimeOfDay(time_text, formats);


/**
 * Formats a date as a string.
 *
 * @param date          The date to be converted.
 * @param format        The format template to use for the conversion;
 *                      see strftime() for appropriate values.  The maximum
 *                      length of the resulting string is 255 characters.
 *                      Optional; defaults to '%Y-%m-%d' which is YYYY-MM-DD.
 * @return              Blank if date cannot be formatted, or the date in the
 *                      requested format.
 */

EXPORT STRING DateToString(Date_t date, VARSTRING format = '%Y-%m-%d') :=
    TimeLib.DateToString(date, format);


/**
 * Formats a time as a string.
 *
 * @param time          The time to be converted.
 * @param format        The format template to use for the conversion;
 *                      see strftime() for appropriate values.  The maximum
 *                      length of the resulting string is 255 characters.
 *                      Optional; defaults to '%H:%M:%S' which is HH:MM:SS.
 * @return              Blank if the time cannot be formatted, or the time
 *                      in the requested format.
 */

EXPORT STRING TimeToString(Time_t time, VARSTRING format = '%H:%M:%S') :=
    TimeLib.TimeToString(time, format);


/**
 * Converts a Seconds_t value into a human-readable string using a format template.
 *
 * @param seconds       The seconds since epoch.
 * @param format        The format template to use for the conversion; see
 *                      strftime() for appropriate values.  The maximum length
 *                      of the resulting string is 255 characters.
 *                      Optional; defaults to '%Y-%m-%dT%H:%M:%S' which is YYYY-MM-DDTHH:MM:SS.
 * @return              The converted seconds as a string.
 */

EXPORT STRING SecondsToString(Seconds_t seconds, VARSTRING format = '%Y-%m-%dT%H:%M:%S') :=
    TimeLib.SecondsToString(seconds, format);


/**
 * Converts a Timestamp_t value into a human-readable string using a format template.
 *
 * @param timestamp     The microseconds since epoch.
 * @param format        The format template to use for the conversion; see
 *                      strftime() for appropriate format specifiers.  Two
 *                      additional format specifiers are available to show
 *                      fractional seconds:
 *                          %@ - fraction of seconds in microseconds (6 digits)
 *                          %# - fraction of seconds in milliseconds (3 digits)
 *                      The maximum length of the resulting string is 255
 *                      characters.  This parameter is optional and defaults to
 *                      '%Y-%m-%dT%H:%M:%S.%@' which is YYYY-MM-DDTHH:MM:SS.ssssss.
 * @return              The converted timestamp as a string.
 */

EXPORT STRING TimestampToString(Timestamp_t timestamp, VARSTRING format = '%Y-%m-%dT%H:%M:%S.%@') := FUNCTION
    ms := INTFORMAT(timestamp % 1000000, 6, 1);
    f2 := REGEXREPLACE('%@', format, ms);
    f3 := REGEXREPLACE('%#', f2, ms[..3]);
    RETURN TimeLib.SecondsToString(timestamp DIV 1000000, f3);
END;


/**
 * Formats a date as a string.
 *
 * @param date          The date to be converted.
 * @param format        The format the date is output in.
 *                      (See documentation for strftime)
 * @return              Blank if date cannot be formatted, or the date in the
 *                      requested format.
 */

EXPORT STRING ToString(Date_t date, VARSTRING format) := DateToString(date, format) : DEPRECATED('Replaced with DateToString() function');


/**
 * Converts a date from one format to another
 *
 * @param date_text     The string containing the date to be converted.
 * @param from_format   The format the date is to be converted from.
 * @param to_format     The format the date is to be converted to.
 * @return              The converted string, or blank if it failed to match the format.
 * @see                 FromStringToDate
 */

EXPORT STRING ConvertDateFormat(STRING date_text, VARSTRING from_format='%m/%d/%Y', VARSTRING to_format='%Y%m%d') := FUNCTION
    parsedDate := FromStringToDate(date_text, from_format);

    reformatResult := IF(parsedDate = (Date_t)0, '', DateToString(parsedDate, to_format));

    RETURN reformatResult;
END;


/**
 * Converts a date from one format to another
 *
 * @param date_text     The string containing the date to be converted.
 * @param from_format   The format the date is to be converted from.
 * @param to_format     The format the date is to be converted to.
 * @return              The converted string, or blank if it failed to match the format.
 */

EXPORT STRING ConvertFormat(STRING date_text, VARSTRING from_format='%m/%d/%Y', VARSTRING to_format='%Y%m%d') :=
    ConvertDateFormat(date_text, from_format, to_format) : DEPRECATED('Replaced with ConvertDateFormat() function');


/**
 * Converts a time from one format to another
 *
 * @param time_text     The string containing the time to be converted.
 * @param from_format   The format the time is to be converted from.
 * @param to_format     The format the time is to be converted to.
 * @return              The converted string, or blank if it failed to match the format.
 */

EXPORT STRING ConvertTimeFormat(STRING time_text, VARSTRING from_format='%H%M%S', VARSTRING to_format='%H:%M:%S') :=
    TimeToString(FromStringToTime(time_text, from_format), to_format);


/**
 * Converts a date that matches one of a set of formats to another.
 *
 * @param date_text     The string containing the date to be converted.
 * @param from_formats  The list of formats the date is to be converted from.
 * @param to_format     The format the date is to be converted to.
 * @return              The converted string, or blank if it failed to match the format.
 * @see                 MatchDateString
 */

EXPORT STRING ConvertDateFormatMultiple(STRING date_text, SET OF VARSTRING from_formats, VARSTRING to_format='%Y%m%d') := FUNCTION
    matchResult := MatchDateString(date_text, from_formats);

    reformatResult := IF(matchResult = (Date_t)0, '', DateToString(matchResult, to_format));

    RETURN reformatResult;
END;


/**
 * Converts a date that matches one of a set of formats to another.
 *
 * @param date_text     The string containing the date to be converted.
 * @param from_formats  The list of formats the date is to be converted from.
 * @param to_format     The format the date is to be converted to.
 * @return              The converted string, or blank if it failed to match the format.
 */

EXPORT STRING ConvertFormatMultiple(STRING date_text, SET OF VARSTRING from_formats, VARSTRING to_format='%Y%m%d') :=
    ConvertDateFormatMultiple(date_text, from_formats, to_format) : DEPRECATED('Replaced with ConvertDateFormatMultiple() function');


/**
 * Converts a time that matches one of a set of formats to another.
 *
 * @param time_text     The string containing the time to be converted.
 * @param from_formats  The list of formats the time is to be converted from.
 * @param to_format     The format the time is to be converted to.
 * @return              The converted string, or blank if it failed to match the format.
 */

EXPORT STRING ConvertTimeFormatMultiple(STRING time_text, SET OF VARSTRING from_formats, VARSTRING to_format='%H:%m:%s') :=
    TimeToString(MatchTimeString(time_text, from_formats), to_format);


/**
 * Adjusts a date by incrementing or decrementing year, month and/or day values.
 * The date must be in the Gregorian calendar after the year 1600.
 * If the new calculated date is invalid then it will be normalized according
 * to mktime() rules.  Example: 20140130 + 1 month = 20140302.
 *
 * @param date          The date to adjust.
 * @param year_delta    The requested change to the year value;
 *                      optional, defaults to zero.
 * @param month_delta   The requested change to the month value;
 *                      optional, defaults to zero.
 * @param day_delta     The requested change to the day of month value;
 *                      optional, defaults to zero.
 * @return              The adjusted Date_t value.
 */

EXPORT Date_t AdjustDate(Date_t date,
                         INTEGER2 year_delta = 0,
                         INTEGER4 month_delta = 0,
                         INTEGER4 day_delta = 0) :=
    TimeLib.AdjustDate(date, year_delta, month_delta, day_delta);


/**
 * Adjusts a date by adding or subtracting seconds.  The date must be in the
 * Gregorian calendar after the year 1600.  If the new calculated
 * date is invalid then it will be normalized according to mktime() rules.
 * Example: 20140130 + 172800 seconds = 20140201.
 *
 * @param date          The date to adjust.
 * @param seconds_delta The requested change to the date, in seconds.
 * @return              The adjusted Date_t value.
 */

EXPORT Date_t AdjustDateBySeconds(Date_t date, INTEGER4 seconds_delta) :=
    TimeLib.AdjustDateBySeconds(date, seconds_delta);


/**
 * Adjusts a time by incrementing or decrementing hour, minute and/or second
 * values.  If the new calculated time is invalid then it will be normalized
 * according to mktime() rules.
 *
 * @param time          The time to adjust.
 * @param hour_delta    The requested change to the hour value;
 *                      optional, defaults to zero.
 * @param minute_delta  The requested change to the minute value;
 *                      optional, defaults to zero.
 * @param second_delta  The requested change to the second of month value;
 *                      optional, defaults to zero.
 * @return              The adjusted Time_t value.
 */

EXPORT Time_t AdjustTime(Time_t time,
                         INTEGER2 hour_delta = 0,
                         INTEGER4 minute_delta = 0,
                         INTEGER4 second_delta = 0) :=
    TimeLib.AdjustTime(time, hour_delta, minute_delta, second_delta);


/**
 * Adjusts a time by adding or subtracting seconds.  If the new calculated
 * time is invalid then it will be normalized according to mktime() rules.
 *
 * @param time          The time to adjust.
 * @param seconds_delta The requested change to the time, in seconds.
 * @return              The adjusted Time_t value.
 */

EXPORT Time_t AdjustTimeBySeconds(Time_t time, INTEGER4 seconds_delta) :=
    TimeLib.AdjustTimeBySeconds(time, seconds_delta);


/**
 * Adjusts a Seconds_t value by adding or subtracting years, months, days,
 * hours, minutes and/or seconds.  This is performed by first converting the
 * seconds into a full date/time structure, applying any delta values to
 * individual date/time components, then converting the structure back to the
 * number of seconds.  This interim date must lie within Gregorian calendar
 * after the year 1600.  If the interim structure is found to have an invalid
 * date/time then it will be normalized according to mktime() rules.  Therefore,
 * some delta values (such as "1 month") are actually relative to the value of
 * the seconds argument.
 *
 * @param seconds       The number of seconds to adjust.
 * @param year_delta    The requested change to the year value;
 *                      optional, defaults to zero.
 * @param month_delta   The requested change to the month value;
 *                      optional, defaults to zero.
 * @param day_delta     The requested change to the day of month value;
 *                      optional, defaults to zero.
 * @param hour_delta    The requested change to the hour value;
 *                      optional, defaults to zero.
 * @param minute_delta  The requested change to the minute value;
 *                      optional, defaults to zero.
 * @param second_delta  The requested change to the second of month value;
 *                      optional, defaults to zero.
 * @return              The adjusted Seconds_t value.
 */

EXPORT Seconds_t AdjustSeconds(Seconds_t seconds,
                               INTEGER2 year_delta = 0,
                               INTEGER4 month_delta = 0,
                               INTEGER4 day_delta = 0,
                               INTEGER4 hour_delta = 0,
                               INTEGER4 minute_delta = 0,
                               INTEGER4 second_delta = 0) :=
    TimeLib.AdjustSeconds(seconds, year_delta, month_delta, day_delta, hour_delta, minute_delta, second_delta);


/**
 * Adjusts a date by incrementing or decrementing months and/or years.  This
 * routine uses the rule outlined in McGinn v. State, 46 Neb. 427, 65 N.W. 46 (1895):
 * "The term calendar month, whether employed in statutes or contracts, and
 * not appearing to have been used in a different sense, denotes a period
 * terminating with the day of the succeeding month numerically corresponding
 * to the day of its beginning, less one. If there be no corresponding day of
 * the succeeding month, it terminates with the last day thereof."  The
 * internet suggests similar legal positions exist in the Commonwealth
 * and Germany.  Note that day adjustments are performed after year and month
 * adjustments using the preceding rules.  As an example, Jan. 31, 2014 + 1 month
 * will result in Feb. 28, 2014; Jan. 31, 2014 + 1 month + 1 day will result
 * in Mar. 1, 2014.
 *
 * @param date          The date to adjust, in the Gregorian calendar after 1600.
 * @param year_delta    The requested change to the year value;
 *                      optional, defaults to zero.
 * @param month_delta   The requested change to the month value;
 *                      optional, defaults to zero.
 * @param day_delta     The requested change to the day value;
 *                      optional, defaults to zero.
 * @return              The adjusted Date_t value.
 */

EXPORT Date_t AdjustCalendar(Date_t date,
                             INTEGER2 year_delta = 0,
                             INTEGER4 month_delta = 0,
                             INTEGER4 day_delta = 0) :=
    TimeLib.AdjustCalendar(date, year_delta, month_delta, day_delta);

/**
 * Helper function.  Calculates the 1-based week number of a date, starting from
 * a reference date.  Week 1 always contains the reference date, and week 2
 * begins on the following day of the week indicated by the value of
 * startingDayOfWeek.  This is not an ISO-8601 implementation of computing week
 * numbers ("week dates").
 *
 * @param date              The date for which to compute the week number;
 *                          must be greater than or equal to referenceDate
 * @param referenceDate     The date from which the week number counting begins;
 *                          must be less than or equal to date
 * @param startingDayOfWeek The index number of the first day of a week, 1-7,
 *                          where 1 = Sunday
 * @return                  The 1-based week number of date, relative to
 *                          referenceDate
 *
 * @see YearWeekNumFromDate, MonthWeekNumFromDate
 */

SHARED WeekNumForDate(Date_t date, Date_t referenceDate, UNSIGNED1 startingDayOfWeek) := FUNCTION
    referenceDayOfWeek := DayOfWeek(referenceDate);
    startingDayOfWeekDelta := (startingDayOfWeek - referenceDayOfWeek) % 7;
    referenceFirstDateOfWeek := AdjustDate(referenceDate, day_delta := startingDayOfWeekDelta);
    numberOfDays := DaysBetween(referenceFirstDateOfWeek, date) + 1;
    weekNum0 := (numberOfDays + 6) DIV 7;
    weekNum := IF(startingDayOfWeek > referenceDayOfWeek, weekNum0 + 1, weekNum0);

    RETURN weekNum;
END;

/**
 * Returns a number representing the day of the week indicated by the given date in ISO-8601.
 * The date must be in the Gregorian calendar after the year 1600.
 *
 * @param date          A Date_t value.
 * @return              A number 1-7 representing the day of the week, where 1 = Monday.
 */
EXPORT ISODayOfWeekFromDate(Date_t d) := ((DayOfWeek(d) + 5) % 7) + 1;		

/**
 * Helper function to figure out the number of weeks for a given year.
 * For more details, see: https://en.wikipedia.org/wiki/ISO_week_date#Weeks_per_year
 *
 * @param date          A Date_t value.
 * @return              Number between 0 and 6 to help figure out whether year is long (53 weeks) or short (52 weeks).
 */
SHARED ISOWeeksP(INTEGER2 year) := (year + TRUNCATE(year/4) - TRUNCATE(year/100) + TRUNCATE(year/400)) % 7;

/**
 * Returns true for years with 53 weeks, and false for years with 52 weeks.
 *
 * @param date          A Date_t value.
 * @return              TRUE if year is a long year (i.e. 53 weeks), FALSE otherwise.
 */
EXPORT ISOIsLongYear(INTEGER2 year) := (ISOWeeksP(year) = 4 OR ISOWeeksP(year - 1) = 3);

/**
 * Returns a number representing the maximum number of weeks for the year from date.
 *
 * @param date          A Date_t value.
 * @return              The number 52 for short years and 53 for long ones.
 */
EXPORT ISOWeeksFromDate(Date_t d) := 52 + IF(ISOIsLongYear(Year(d)), 1, 0);

/**
 * Returns a number representing the raw week number of the given date.
 *
 * @param date          A Date_t value.
 * @return              A number from 1 to 53.
 */
EXPORT ISORawWeekNumForDate(Date_t d) := TRUNCATE((DayOfYear(d) - ISODayOfWeekFromDate(d) + 10) / 7);

/**
 * Returns the ISO 1-based week number and year, of that week number, of a date.
 * First day(s) of a year may be in the previous year's last week number.
 * This is an ISO-8601 implementation of computing week numbers ("week dates").
 *
 * @param date          A Date_t value.
 * @return              A number 1-53 representing the week number in a year, 
 *                      and year 1600+ representing the year of that week number
 *                      (could be previous year from given date).
 */
EXPORT ISOWeekNumWeekDayAndYearFromDate(Date_t d) := FUNCTION
    givenYear := Year(d); 
    lastDayPreviousYear := DateFromParts(givenYear - 1, 12, 31);
    lastWeekPreviousYear := ISOWeeksFromDate(lastDayPreviousYear);
    lastDayGivenYear := DateFromParts(givenYear, 12, 31);
    lastWeekGivenYear := ISOWeeksFromDate(lastDayGivenYear);
    rawWeekNumber := ISORawWeekNumForDate(d);
    weekNumber := IF(rawWeekNumber < 1, lastWeekPreviousYear, IF(rawWeekNumber > lastWeekGivenYear, 1, rawWeekNumber));
    weekNumberYear := (givenYear + IF(rawWeekNumber < 1, -1, IF(rawWeekNumber > lastWeekGivenYear, 1, 0)));
    weekDay := ISODayOfWeekFromDate(d);
    result := MODULE
        EXPORT weekNumber := weekNumber;
        EXPORT year := weekNumberYear;
        EXPORT weekDay := weekDay;
    END;
    RETURN result;
END;

/**
 * Returns the ISO-8601 week date in extended (e.g. 2018-W23-7) or 
 * compact (e.g. 2018W237) form.
 * This is an ISO-8601 implementation of computing week numbers ("week dates").
 *
 * @param date          A Date_t value.
 * @return              A number 1-53 representing the week number in a year, 
 *                      and year 1600+ representing the year of that week number
 *                      (could be previous year from given date).
 */
EXPORT ISOWeekDate(Date_t d, BOOLEAN extended = FALSE) := FUNCTION
    ISOWeekNumWeekDayAndYear := ISOWeekNumWeekDayAndYearFromDate(d);
    sep := IF(extended, '-', '');
    RETURN INTFORMAT(ISOWeekNumWeekDayAndYear.year, 4, 1) + sep + 'W' + INTFORMAT(ISOWeekNumWeekDayAndYear.weeknumber, 2, 1) + sep + ISOWeekNumWeekDayAndYear.weekday;
END;

/**
 * Returns the 1-based week number of a date within the date's year.  Week 1
 * always contains the first day of the year, and week 2 begins on the
 * following day of the week indicated by the value of startingDayOfWeek.  This
 * is not an ISO-8601 implementation of computing week numbers ("week dates").
 *
 * @param date              The date for which to compute the week number
 * @param startingDayOfWeek The index number of the first day of a week, 1-7,
 *                          where 1 = Sunday; OPTIONAL, defaults to 1
 * @return                  The 1-based week number of date, relative to
 *                          the beginning of the date's year
 *
 * @see MonthWeekNumFromDate
 */

EXPORT YearWeekNumFromDate(Date_t date, UNSIGNED1 startingDayOfWeek = 1) := FUNCTION
    yearStart := DateFromParts(Year(date), 1, 1);

    RETURN WeekNumForDate(date, yearStart, startingDayOfWeek);
END;

/**
 * Returns the 1-based week number of a date within the date's month.  Week 1
 * always contains the first day of the month, and week 2 begins on the
 * following day of the week indicated by the value of startingDayOfWeek.  This
 * is not an ISO-8601 implementation of computing week numbers ("week dates").
 *
 * @param date              The date for which to compute the week number
 * @param startingDayOfWeek The index number of the first day of a week, 1-7,
 *                          where 1 = Sunday; OPTIONAL, defaults to 1
 * @return                  The 1-based week number of date, relative to
 *                          the beginning of the date's month
 *
 * @see YearWeekNumFromDate
 */

EXPORT MonthWeekNumFromDate(Date_t date, UNSIGNED1 startingDayOfWeek = 1) := FUNCTION
    monthStart := DateFromParts(Year(date), Month(date), 1);

    RETURN WeekNumForDate(date, monthStart, startingDayOfWeek);
END;


/**
 * Returns a boolean indicating whether daylight savings time is currently
 * in effect locally.
 *
 * @return      TRUE if daylight savings time is currently in effect, FALSE otherwise.
 */

EXPORT BOOLEAN IsLocalDaylightSavingsInEffect() :=
    TimeLib.IsLocalDaylightSavingsInEffect();


/**
 * Returns the offset (in seconds) of the time represented from UTC, with
 * positive values indicating locations east of the Prime Meridian.  Given a
 * UTC time in seconds since epoch, you can find the local time by adding the
 * result of this function to the seconds.  Note that daylight savings time is
 * factored into the offset.
 *
 * @return              The number of seconds offset from UTC.
 */

EXPORT INTEGER4 LocalTimeZoneOffset() :=
    TimeLib.LocalTimeZoneOffset();


/**
 * Returns the current date.
 *
 * @param in_local_time     TRUE if the returned value should be local to the
 *                          cluster computing the date, FALSE for UTC.
 *                          Optional, defaults to FALSE.
 * @return                  A Date_t representing the current date.
 */

EXPORT Date_t CurrentDate(BOOLEAN in_local_time = FALSE) :=
    TimeLib.CurrentDate(in_local_time);


/**
 * Returns the current date in the local time zone.
 *
 * @return              A Date_t representing the current date.
 */

EXPORT Date_t Today() := CurrentDate(TRUE);


/**
 * Returns the current time of day
 *
 * @param in_local_time     TRUE if the returned value should be local to the
 *                          cluster computing the time, FALSE for UTC.
 *                          Optional, defaults to FALSE.
 * @return                  A Time_t representing the current time of day.
 */

EXPORT Time_t CurrentTime(BOOLEAN in_local_time = FALSE) :=
    TimeLib.CurrentTime(in_local_time);


/**
 * Returns the current date and time as the number of seconds since epoch.
 *
 * @param in_local_time     TRUE if the returned value should be local to the
 *                          cluster computing the time, FALSE for UTC.
 *                          Optional, defaults to FALSE.
 * @return                  A Seconds_t representing the current time in
 *                          UTC or local time, depending on the argument.
 */

EXPORT Seconds_t CurrentSeconds(BOOLEAN in_local_time = FALSE) :=
    TimeLib.CurrentSeconds(in_local_time);


/**
 * Returns the current date and time as the number of microseconds since epoch.
 *
 * @param in_local_time     TRUE if the returned value should be local to the
 *                          cluster computing the time, FALSE for UTC.
 *                          Optional, defaults to FALSE.
 * @return                  A Timestamp_t representing the current time in
 *                          microseconds in UTC or local time, depending on
 *                          the argument.
 */

EXPORT Timestamp_t CurrentTimestamp(BOOLEAN in_local_time = FALSE) :=
    TimeLib.CurrentTimestamp(in_local_time);


/**
 * Returns the beginning and ending dates for the month surrounding the given date.
 *
 * @param as_of_date    The reference date from which the month will be
 *                      calculated.  This date must be a date within the
 *                      Gregorian calendar.  Optional, defaults to the
 *                      current date in UTC.
 * @return              Module with exported attributes for startDate and endDate.
 */

EXPORT DatesForMonth(Date_t as_of_date = CurrentDate(FALSE)) := FUNCTION
    lastDay := TimeLib.GetLastDayOfMonth(as_of_date);
    firstDay := (lastDay DIV 100) * 100 + 1;

    result := MODULE
        EXPORT Date_t startDate := firstDay;
        EXPORT Date_t endDate := lastDay;
    END;

    RETURN result;
END;


/**
 * Returns the beginning and ending dates for the week surrounding the given date
 * (Sunday marks the beginning of a week).
 *
 * @param as_of_date    The reference date from which the week will be
 *                      calculated.  This date must be a date within the
 *                      Gregorian calendar.  Optional, defaults to the
 *                      current date in UTC.
 * @return              Module with exported attributes for startDate and endDate.
 */

EXPORT DatesForWeek(Date_t as_of_date = CurrentDate(FALSE)) := FUNCTION
    lastWeekDates := ROW(TimeLib.DatesForWeek(as_of_date));

    result := MODULE
        EXPORT Date_t startDate := lastWeekDates.startDate;
        EXPORT Date_t endDate := lastWeekDates.endDate;
    END;

    RETURN result;
END;


/**
 * Tests whether a date is valid, both by range-checking the year and by
 * validating each of the other individual components.
 *
 * @param date              The date to validate.
 * @param yearLowerBound    The minimum acceptable year.
 *                          Optional; defaults to 1800.
 * @param yearUpperBound    The maximum acceptable year.
 *                          Optional; defaults to 2100.
 * @return                  TRUE if the date is valid, FALSE otherwise.
 */

EXPORT BOOLEAN IsValidDate(Date_t date,
                           INTEGER2 yearLowerBound = 1800,
                           INTEGER2 yearUpperBound = 2100) := FUNCTION
    yearInBounds := (Year(date) BETWEEN yearLowerBound AND yearUpperBound);
    monthInBounds := (Month(date) BETWEEN 1 AND 12);
    maxDayInMonth := CHOOSE(Month(date),31,IF(IsLeapYear(Year(date)),29,28),31,30,31,30,31,31,30,31,30,31);
    dayInBounds := (Day(date) BETWEEN 1 AND maxDayInMonth);

    RETURN yearInBounds AND monthInBounds AND dayInBounds;
END;


/**
 * Tests whether a date is valid in the Gregorian calendar.  The year
 * must be between 1601 and 30827.
 *
 * @param date              The Date_t to validate.
 * @return                  TRUE if the date is valid, FALSE otherwise.
 */

EXPORT BOOLEAN IsValidGregorianDate(Date_t date) := FUNCTION
    yearInBounds := (Year(date) BETWEEN 1601 AND 30827);
    matchesNormalized := (date = AdjustDate(date)); // AdjustDate normalizes, so this is a validation check

    RETURN yearInBounds AND matchesNormalized;
END;


/**
 * Tests whether a time is valid.
 *
 * @param time              The time to validate.
 * @return                  TRUE if the time is valid, FALSE otherwise.
 */

EXPORT BOOLEAN IsValidTime(Time_t time) := FUNCTION
    hourInBounds := (Hour(time) <= 23);
    minuteInBounds := (Minute(time) <= 59);
    secondInBounds := (Second(time) <= 59);

    RETURN hourInBounds AND minuteInBounds AND secondInBounds;
END;

//------------------------------------------------------------------------------
// Transforms
//------------------------------------------------------------------------------

/**
 * A transform to create a Date_rec from the individual elements
 *
 * @param year          The year
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @return              A transform that creates a Date_rec containing the date.
 */

EXPORT Date_rec CreateDate(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := TRANSFORM
    SELF.year := year;
    SELF.month := month;
    SELF.day := day;
END;


/**
 * A transform to create a Date_rec from a Seconds_t value.
 *
 * @param seconds               The number seconds since epoch.
 * @param is_local_time         TRUE if seconds is expressed in local time
 *                              rather than UTC, FALSE if seconds is expressed
 *                              in UTC.  Optional, defaults to FALSE.
 * @return                      A transform that creates a Date_rec containing
 *                              the date.
 */

EXPORT Date_rec CreateDateFromSeconds(Seconds_t seconds, BOOLEAN is_local_time = FALSE) := TRANSFORM
    timeParts := SecondsToParts(seconds, is_local_time);

    SELF.year := timeParts.year;
    SELF.month := timeParts.month;
    SELF.day := timeParts.day;
END;


/**
 * A transform to create a Time_rec from the individual elements
 *
 * @param hour          The hour (0-23).
 * @param minute        The minute (0-59).
 * @param second        The second (0-59).
 * @return              A transform that creates a Time_rec containing the time of day.
 */

EXPORT Time_rec CreateTime(UNSIGNED1 hour, UNSIGNED1 minute, UNSIGNED1 second) := TRANSFORM
    SELF.hour := hour;
    SELF.minute := minute;
    SELF.second := second;
END;


/**
 * A transform to create a Time_rec from a Seconds_t value.
 *
 * @param seconds               The number seconds since epoch.
 * @param is_local_time         TRUE if seconds is expressed in local time
 *                              rather than UTC, FALSE if seconds is expressed
 *                              in UTC.  Optional, defaults to FALSE.
 * @return                      A transform that creates a Time_rec containing
 *                              the time of day.
 */

EXPORT Time_rec CreateTimeFromSeconds(Seconds_t seconds, BOOLEAN is_local_time = FALSE) := TRANSFORM
    timeParts := SecondsToParts(seconds, is_local_time);

    SELF.hour := timeParts.hour;
    SELF.minute := timeParts.minute;
    SELF.second := timeParts.second;
END;


/**
 * A transform to create a DateTime_rec from the individual elements
 *
 * @param year          The year
 * @param month         The month (1-12).
 * @param day           The day (1..daysInMonth).
 * @param hour          The hour (0-23).
 * @param minute        The minute (0-59).
 * @param second        The second (0-59).
 * @return              A transform that creates a DateTime_rec containing date
 *                      and time components.
 */

EXPORT DateTime_rec CreateDateTime(INTEGER2 year,
                                   UNSIGNED1 month,
                                   UNSIGNED1 day,
                                   UNSIGNED1 hour,
                                   UNSIGNED1 minute,
                                   UNSIGNED1 second) := TRANSFORM
    SELF.year := year;
    SELF.month := month;
    SELF.day := day;
    SELF.hour := hour;
    SELF.minute := minute;
    SELF.second := second;
END;


/**
 * A transform to create a DateTime_rec from a Seconds_t value.
 *
 * @param seconds               The number seconds since epoch.
 * @param is_local_time         TRUE if seconds is expressed in local time
 *                              rather than UTC, FALSE if seconds is expressed
 *                              in UTC.  Optional, defaults to FALSE.
 * @return                      A transform that creates a DateTime_rec
 *                              containing date and time components.
 */

EXPORT DateTime_rec CreateDateTimeFromSeconds(Seconds_t seconds, BOOLEAN is_local_time = FALSE) := TRANSFORM
    timeParts := SecondsToParts(seconds, is_local_time);

    SELF.year := timeParts.year;
    SELF.month := timeParts.month;
    SELF.day := timeParts.day;
    SELF.hour := timeParts.hour;
    SELF.minute := timeParts.minute;
    SELF.second := timeParts.second;
END;

//------------------------------------------------------------------------------
// Time Zone Module
//------------------------------------------------------------------------------

EXPORT TimeZone := MODULE, FORWARD

/**
 * Record definition for exported time zone information
 */

EXPORT TZDataLayout := RECORD
    STRING5         tzAbbrev;       // Time zone abbreviation; always uppercase; may be duplicated between records
    INTEGER4        secondsOffset;  // Number of seconds east (positive) or west (negative) of UTC
    SET OF STRING15 locations;      // Names of locations that use the given time zone abbreviation
END;

/**
 * Hardcoded time zone definitions; a general description of each time zone
 * is included as a line comment.  This information was collected from
 * https://www.timeanddate.com/time/zones/ with one modification (see below).
 *
 * The IST abbreviation can indicate three different time zones:
 *      India Standard Time
 *      Irish Standard Time
 *      Israel Standard Time
 *
 * Unfortunately, two of those IST time zones lie in the same location:  ASIA.
 * That makes it impossible to differentiate between them, and they have very
 * different offsets.  As a consequence, locations for Israel Standard Time and
 * Israel Daylight Time have been changed from ASIA to ISRAEL.
 */

EXPORT TZ_DATA := DATASET
    (
        [
            {'A', 3600, ['MILITARY']}, // Alpha Time Zone
            {'ACDT', 37800, ['AUSTRALIA']}, // Australian Central Daylight Time
            {'ACST', 34200, ['AUSTRALIA']}, // Australian Central Standard Time
            {'ACT', -18000, ['SOUTH AMERICA']}, // Acre Time
            {'ACT', 34200, ['AUSTRALIA']}, // Australian Central Time
            {'ACWST', 31500, ['AUSTRALIA']}, // Australian Central Western Standard Time
            {'ADT', 10800, ['ASIA']}, // Arabia Daylight Time
            {'ADT', -10800, ['NORTH AMERICA', 'ATLANTIC']}, // Atlantic Daylight Time
            {'AEDT', 39600, ['AUSTRALIA']}, // Australian Eastern Daylight Time
            {'AEST', 36000, ['AUSTRALIA']}, // Australian Eastern Standard Time
            {'AET', 36000, ['AUSTRALIA']}, // Australian Eastern Time
            {'AFT', 16200, ['ASIA']}, // Afghanistan Time
            {'AKDT', -28800, ['NORTH AMERICA']}, // Alaska Daylight Time
            {'AKST', -32400, ['NORTH AMERICA']}, // Alaska Standard Time
            {'ALMT', 21600, ['ASIA']}, // Alma-Ata Time
            {'AMST', -10800, ['SOUTH AMERICA']}, // Amazon Summer Time
            {'AMST', 18000, ['ASIA']}, // Armenia Summer Time
            {'AMT', -14400, ['SOUTH AMERICA']}, // Amazon Time
            {'AMT', 14400, ['ASIA']}, // Armenia Time
            {'ANAST', 43200, ['ASIA']}, // Anadyr Summer Time
            {'ANAT', 43200, ['ASIA']}, // Anadyr Time
            {'AOE', -43200, ['PACIFIC']}, // Anywhere on Earth
            {'AQTT', 18000, ['ASIA']}, // Aqtobe Time
            {'ART', -10800, ['ANTARCTICA', 'SOUTH AMERICA']}, // Argentina Time
            {'AST', 7200, ['ASIA']}, // Arabia Standard Time
            {'AST', -14400, ['NORTH AMERICA', 'ATLANTIC' , 'CARIBBEAN']}, // Atlantic Standard Time
            {'AT', -14400, ['NORTH AMERICA', 'ATLANTIC', 'CARIBBEAN']}, // Atlantic Time
            {'AWDT', 32400, ['AUSTRALIA']}, // Australian Western Daylight Time
            {'AWST', 28800, ['AUSTRALIA']}, // Australian Western Standard Time
            {'AZOST', 0, ['ATLANTIC']}, // Azores Summer Time
            {'AZOT', -3600, ['ATLANTIC']}, // Azores Time
            {'AZST', 18000, ['ASIA']}, // Azerbaijan Summer Time
            {'AZT', 14400, ['ASIA']}, // Azerbaijan Time
            {'B', 7200, ['MILITARY']}, // Bravo Time Zone
            {'BNT', 28800, ['ASIA']}, // Brunei Darussalam Time
            {'BOT', -14400, ['SOUTH AMERICA']}, // Bolivia Time
            {'BRST', -7200, ['SOUTH AMERICA']}, // Brazil Summer Time
            {'BRT', -10800, ['SOUTH AMERICA']}, // Brazil Time
            {'BST', 21600, ['ASIA']}, // Bangladesh Standard Time
            {'BST', 39600, ['PACIFIC']}, // Bougainville Standard Time
            {'BST', 3600, ['EUROPE']}, // British Summer Time
            {'BTT', 21600, ['ASIA']}, // Bhutan Time
            {'C', 10800, ['MILITARY']}, // Charlie Time Zone
            {'CAST', 28800, ['ANTARCTICA']}, // Casey Time
            {'CAT', 7200, ['AFRICA']}, // Central Africa Time
            {'CCT', 23400, ['INDIAN OCEAN']}, // Cocos Islands Time
            {'CDT', -18000, ['NORTH AMERICA']}, // Central Daylight Time
            {'CDT', -14400, ['CARIBBEAN']}, // Cuba Daylight Time
            {'CEST', 7200, ['EUROPE', 'ANTARCTICA']}, // Central European Summer Time
            {'CET', 3600, ['EUROPE', 'AFRICA']}, // Central European Time
            {'CHADT', 49500, ['PACIFIC']}, // Chatham Island Daylight Time
            {'CHAST', 45900, ['PACIFIC']}, // Chatham Island Standard Time
            {'CHOST', 32400, ['ASIA']}, // Choibalsan Summer Time
            {'CHOT', 28800, ['ASIA']}, // Choibalsan Time
            {'ChST', 36000, ['PACIFIC']}, // Chamorro Standard Time
            {'CHUT', 36000, ['PACIFIC']}, // Chuuk Time
            {'CIDST', -14400, ['CARIBBEAN']}, // Cayman Islands Daylight Saving Time
            {'CIST', -18000, ['CARIBBEAN']}, // Cayman Islands Standard Time
            {'CKT', -36000, ['PACIFIC']}, // Cook Island Time
            {'CLST', -10800, ['SOUTH AMERICA', 'ANTARCTICA']}, // Chile Summer Time
            {'CLT', -14400, ['SOUTH AMERICA', 'ANTARCTICA']}, // Chile Standard Time
            {'COT', -18000, ['SOUTH AMERICA']}, // Colombia Time
            {'CST', -21600, ['NORTH AMERICA', 'CENTRAL AMERICA']}, // Central Standard Time
            {'CST', 28800, ['ASIA']}, // China Standard Time
            {'CST', -18000, ['CARIBBEAN']}, // Cuba Standard Time
            {'CT', -21600, ['NORTH AMERICA', 'CENTRAL AMERICA']}, // Central Time
            {'CVT', -3600, ['AFRICA']}, // Cape Verde Time
            {'CXT', 25200, ['AUSTRALIA']}, // Christmas Island Time
            {'D', 14400, ['MILITARY']}, // Delta Time Zone
            {'DAVT', 25200, ['ANTARCTICA']}, // Davis Time
            {'DDUT', 36000, ['ANTARCTICA']}, // Dumont-d'Urville Time
            {'E', 18000, ['MILITARY']}, // Echo Time Zone
            {'EASST', -18000, ['PACIFIC']}, // Easter Island Summer Time
            {'EAST', -21600, ['PACIFIC']}, // Easter Island Standard Time
            {'EAT', 10800, ['AFRICA', 'INDIAN OCEAN']}, // Eastern Africa Time
            {'ECT', -18000, ['SOUTH AMERICA']}, // Ecuador Time
            {'EDT', -14400, ['NORTH AMERICA', 'CARIBBEAN']}, // Eastern Daylight Time
            {'EEST', 10800, ['EUROPE', 'ASIA']}, // Eastern European Summer Time
            {'EET', 7200, ['EUROPE', 'ASIA', 'AFRICA']}, // Eastern European Time
            {'EGST', 0, ['NORTH AMERICA']}, // Eastern Greenland Summer Time
            {'EGT', -3600, ['NORTH AMERICA']}, // East Greenland Time
            {'EST', -18000, ['NORTH AMERICA', 'CARIBBEAN', 'CENTRAL AMERICA']}, // Eastern Standard Time
            {'ET', -18000, ['NORTH AMERICA', 'CARIBBEAN', 'CENTRAL AMERICA']}, // Eastern Time
            {'F', 21600, ['MILITARY']}, // Foxtrot Time Zone
            {'FET', 10800, ['EUROPE']}, // Further-Eastern European Time
            {'FJST', 46800, ['PACIFIC']}, // Fiji Summer Time
            {'FJT', 43200, ['PACIFIC']}, // Fiji Time
            {'FKST', -10800, ['SOUTH AMERICA']}, // Falkland Islands Summer Time
            {'FKT', -14400, ['SOUTH AMERICA']}, // Falkland Island Time
            {'FNT', -7200, ['SOUTH AMERICA']}, // Fernando de Noronha Time
            {'G', 25200, ['MILITARY']}, // Golf Time Zone
            {'GALT', -21600, ['PACIFIC']}, // Galapagos Time
            {'GAMT', -32400, ['PACIFIC']}, // Gambier Time
            {'GET', 14400, ['ASIA']}, // Georgia Standard Time
            {'GFT', -10800, ['SOUTH AMERICA']}, // French Guiana Time
            {'GILT', 43200, ['PACIFIC']}, // Gilbert Island Time
            {'GMT', 0, ['EUROPE', 'AFRICA', 'NORTH AMERICA', 'ANTARCTICA']}, // Greenwich Mean Time
            {'GST', 14400, ['ASIA']}, // Gulf Standard Time
            {'GST', -7200, ['SOUTH AMERICA']}, // South Georgia Time
            {'GYT', -14400, ['SOUTH AMERICA']}, // Guyana Time
            {'H', 28800, ['MILITARY']}, // Hotel Time Zone
            {'HADT', -32400, ['NORTH AMERICA']}, // Hawaii-Aleutian Daylight Time
            {'HAST', -36000, ['NORTH AMERICA', 'PACIFIC']}, // Hawaii-Aleutian Standard Time
            {'HKT', 28800, ['ASIA']}, // Hong Kong Time
            {'HOVST', 28800, ['ASIA']}, // Hovd Summer Time
            {'HOVT', 25200, ['ASIA']}, // Hovd Time
            {'I', 32400, ['MILITARY']}, // India Time Zone
            {'ICT', 25200, ['ASIA']}, // Indochina Time
            {'IDT', 10800, ['ISRAEL']}, // Israel Daylight Time; location was ASIA
            {'IOT', 21600, ['INDIAN OCEAN']}, // Indian Chagos Time
            {'IRDT', 16200, ['ASIA']}, // Iran Daylight Time
            {'IRKST', 32400, ['ASIA']}, // Irkutsk Summer Time
            {'IRKT', 28800, ['ASIA']}, // Irkutsk Time
            {'IRST', 12600, ['ASIA']}, // Iran Standard Time
            {'IST', 19800, ['ASIA']}, // India Standard Time
            {'IST', 3600, ['EUROPE']}, // Irish Standard Time
            {'IST', 7200, ['ISRAEL']}, // Israel Standard Time; location was ASIA
            {'JST', 32400, ['ASIA']}, // Japan Standard Time
            {'K', 36000, ['MILITARY']}, // Kilo Time Zone
            {'KGT', 21600, ['ASIA']}, // Kyrgyzstan Time
            {'KOST', 39600, ['PACIFIC']}, // Kosrae Time
            {'KRAST', 28800, ['ASIA']}, // Krasnoyarsk Summer Time
            {'KRAT', 25200, ['ASIA']}, // Krasnoyarsk Time
            {'KST', 32400, ['ASIA']}, // Korea Standard Time
            {'KUYT', 14400, ['EUROPE']}, // Kuybyshev Time
            {'L', 39600, ['MILITARY']}, // Lima Time Zone
            {'LHDT', 39600, ['AUSTRALIA']}, // Lord Howe Daylight Time
            {'LHST', 37800, ['AUSTRALIA']}, // Lord Howe Standard Time
            {'LINT', 50400, ['PACIFIC']}, // Line Islands Time
            {'M', 43200, ['MILITARY']}, // Mike Time Zone
            {'MAGST', 43200, ['ASIA']}, // Magadan Summer Time
            {'MAGT', 39600, ['ASIA']}, // Magadan Time
            {'MART', -34200, ['PACIFIC']}, // Marquesas Time
            {'MAWT', 18000, ['ANTARCTICA']}, // Mawson Time
            {'MDT', -21600, ['NORTH AMERICA']}, // Mountain Daylight Time
            {'MHT', 43200, ['PACIFIC']}, // Marshall Islands Time
            {'MMT', 23400, ['ASIA']}, // Myanmar Time
            {'MSD', 14400, ['EUROPE']}, // Moscow Daylight Time
            {'MSK', 10800, ['EUROPE', 'ASIA']}, // Moscow Standard Time
            {'MST', -25200, ['NORTH AMERICA']}, // Mountain Standard Time
            {'MT', -25200, ['NORTH AMERICA']}, // Mountain Time
            {'MUT', 14400, ['AFRICA']}, // Mauritius Time
            {'MVT', 18000, ['ASIA']}, // Maldives Time
            {'MYT', 28800, ['ASIA']}, // Malaysia Time
            {'N', -3600, ['MILITARY']}, // November Time Zone
            {'NCT', 39600, ['PACIFIC']}, // New Caledonia Time
            {'NDT', -9000, ['NORTH AMERICA']}, // Newfoundland Daylight Time
            {'NFT', 39600, ['AUSTRALIA']}, // Norfolk Time
            {'NOVST', 25200, ['ASIA']}, // Novosibirsk Summer Time
            {'NOVT', 21600, ['ASIA']}, // Novosibirsk Time
            {'NPT', 20700, ['ASIA']}, // Nepal Time
            {'NRT', 43200, ['PACIFIC']}, // Nauru Time
            {'NST', -12600, ['NORTH AMERICA']}, // Newfoundland Standard Time
            {'NUT', -39600, ['PACIFIC']}, // Niue Time
            {'NZDT', 46800, ['PACIFIC', 'ANTARCTICA']}, // New Zealand Daylight Time
            {'NZST', 43200, ['PACIFIC', 'ANTARCTICA']}, // New Zealand Standard Time
            {'O', -7200, ['MILITARY']}, // Oscar Time Zone
            {'OMSST', 25200, ['ASIA']}, // Omsk Summer Time
            {'OMST', 21600, ['ASIA']}, // Omsk Standard Time
            {'ORAT', 18000, ['ASIA']}, // Oral Time
            {'P', -10800, ['MILITARY']}, // Papa Time Zone
            {'PDT', -25200, ['NORTH AMERICA']}, // Pacific Daylight Time
            {'PET', -18000, ['SOUTH AMERICA']}, // Peru Time
            {'PETST', 43200, ['ASIA']}, // Kamchatka Summer Time
            {'PETT', 43200, ['ASIA']}, // Kamchatka Time
            {'PGT', 36000, ['PACIFIC']}, // Papua New Guinea Time
            {'PHOT', 46800, ['PACIFIC']}, // Phoenix Island Time
            {'PHT', 28800, ['ASIA']}, // Philippine Time
            {'PKT', 18000, ['ASIA']}, // Pakistan Standard Time
            {'PMDT', -7200, ['NORTH AMERICA']}, // Pierre & Miquelon Daylight Time
            {'PMST', -10800, ['NORTH AMERICA']}, // Pierre & Miquelon Standard Time
            {'PONT', 39600, ['PACIFIC']}, // Pohnpei Standard Time
            {'PST', -28800, ['NORTH AMERICA']}, // Pacific Standard Time
            {'PST', -28800, ['PACIFIC']}, // Pitcairn Standard Time
            {'PT', -28800, ['NORTH AMERICA']}, // Pacific Time
            {'PWT', 32400, ['PACIFIC']}, // Palau Time
            {'PYST', -10800, ['SOUTH AMERICA']}, // Paraguay Summer Time
            {'PYT', -14400, ['SOUTH AMERICA']}, // Paraguay Time
            {'PYT', 30600, ['ASIA']}, // Pyongyang Time
            {'Q', -14400, ['MILITARY']}, // Quebec Time Zone
            {'QYZT', 21600, ['ASIA']}, // Qyzylorda Time
            {'R', -18000, ['MILITARY']}, // Romeo Time Zone
            {'RET', 14400, ['AFRICA']}, // Reunion Time
            {'ROTT', -10800, ['ANTARCTICA']}, // Rothera Time
            {'S', -21600, ['MILITARY']}, // Sierra Time Zone
            {'SAKT', 39600, ['ASIA']}, // Sakhalin Time
            {'SAMT', 14400, ['EUROPE']}, // Samara Time
            {'SAST', 7200, ['AFRICA']}, // South Africa Standard Time
            {'SBT', 39600, ['PACIFIC']}, // Solomon Islands Time
            {'SCT', 14400, ['AFRICA']}, // Seychelles Time
            {'SGT', 28800, ['ASIA']}, // Singapore Time
            {'SRET', 39600, ['ASIA']}, // Srednekolymsk Time
            {'SRT', -10800, ['SOUTH AMERICA']}, // Suriname Time
            {'SST', -39600, ['PACIFIC']}, // Samoa Standard Time
            {'SYOT', 10800, ['ANTARCTICA']}, // Syowa Time
            {'T', -25200, ['MILITARY']}, // Tango Time Zone
            {'TAHT', -36000, ['PACIFIC']}, // Tahiti Time
            {'TFT', 18000, ['INDIAN OCEAN']}, // French Southern and Antarctic Time
            {'TJT', 18000, ['ASIA']}, // Tajikistan Time
            {'TKT', 46800, ['PACIFIC']}, // Tokelau Time
            {'TLT', 32400, ['ASIA']}, // East Timor Time
            {'TMT', 18000, ['ASIA']}, // Turkmenistan Time
            {'TOST', 50400, ['PACIFIC']}, // Tonga Summer Time
            {'TOT', 46800, ['PACIFIC']}, // Tonga Time
            {'TRT', 10800, ['ASIA', 'EUROPE']}, // Turkey Time
            {'TVT', 43200, ['PACIFIC']}, // Tuvalu Time
            {'U', -28800, ['MILITARY']}, // Uniform Time Zone
            {'ULAST', 32400, ['ASIA']}, // Ulaanbaatar Summer Time
            {'ULAT', 28800, ['ASIA']}, // Ulaanbaatar Time
            {'UTC', 0, ['WORLDWIDE']}, // Coordinated Universal Time
            {'UYST', -7200, ['SOUTH AMERICA']}, // Uruguay Summer Time
            {'UYT', -10800, ['SOUTH AMERICA']}, // Uruguay Time
            {'UZT', 18000, ['ASIA']}, // Uzbekistan Time
            {'V', -32400, ['MILITARY']}, // Victor Time Zone
            {'VET', -14400, ['SOUTH AMERICA']}, // Venezuelan Standard Time
            {'VLAST', 39600, ['ASIA']}, // Vladivostok Summer Time
            {'VLAT', 36000, ['ASIA']}, // Vladivostok Time
            {'VOST', 21600, ['ANTARCTICA']}, // Vostok Time
            {'VUT', 39600, ['PACIFIC']}, // Vanuatu Time
            {'W', -36000, ['MILITARY']}, // Whiskey Time Zone
            {'WAKT', 43200, ['PACIFIC']}, // Wake Time
            {'WARST', -10800, ['SOUTH AMERICA']}, // Western Argentine Summer Time
            {'WAST', 7200, ['AFRICA']}, // West Africa Summer Time
            {'WAT', 3600, ['AFRICA']}, // West Africa Time
            {'WEST', 3600, ['EUROPE', 'AFRICA']}, // Western European Summer Time
            {'WET', 0, ['EUROPE', 'AFRICA']}, // Western European Time
            {'WFT', 43200, ['PACIFIC']}, // Wallis and Futuna Time
            {'WGST', -7200, ['NORTH AMERICA']}, // Western Greenland Summer Time
            {'WGT', -10800, ['NORTH AMERICA']}, // West Greenland Time
            {'WIB', 25200, ['ASIA']}, // Western Indonesian Time
            {'WIT', 32400, ['ASIA']}, // Eastern Indonesian Time
            {'WITA', 28800, ['ASIA']}, // Central Indonesian Time
            {'WST', 50400, ['PACIFIC']}, // West Samoa Time
            {'WST', 3600, ['AFRICA']}, // Western Sahara Summer Time
            {'WT', 0, ['AFRICA']}, // Western Sahara Standard Time
            {'X', -39600, ['MILITARY']}, // X-ray Time Zone
            {'Y', -43200, ['MILITARY']}, // Yankee Time Zone
            {'YAKST', 36000, ['ASIA']}, // Yakutsk Summer Time
            {'YAKT', 32400, ['ASIA']}, // Yakutsk Time
            {'YAPT', 36000, ['PACIFIC']}, // Yap Time
            {'YEKST', 21600, ['ASIA']}, // Yekaterinburg Summer Time
            {'YEKT', 18000, ['ASIA']}, // Yekaterinburg Time
            {'Z', 0, ['MILITARY']} // Zulu Time Zone
        ],
        TZDataLayout
    );

/**
 * Return a list of unique time zone abbreviations from the hardcoded dataset.
 * All abbreviations are in uppercase.
 *
 * @return              A new DATASET({STRING5 tzAbbrev}) containing the
 *                      unique time zone abbreviations.
 */

EXPORT UniqueTZAbbreviations() := FUNCTION
    RETURN TABLE(TZ_DATA, {tzAbbrev}, tzAbbrev);
END;

/**
 * Return a list of unique location names from the hardcoded dataset.
 * All names are in uppercase.
 *
 * @return              A new DATASET({STRING name}) containing the
 *                      unique location names.
 */

EXPORT UniqueTZLocations() := FUNCTION
    NameRec := {STRING name};

    // Gather all locations as a collection of child datasets
    collectedNames := PROJECT
        (
            TZ_DATA,
            TRANSFORM
                (
                    {
                        DATASET(NameRec)    names
                    },
                    SELF.names := DATASET(LEFT.locations, NameRec)
                )
        );

    // Flatten collected names, so there is one name per record
    flattenedNames := NORMALIZE
        (
            collectedNames,
            LEFT.names,
            TRANSFORM
                (
                    NameRec,
                    SELF.name := RIGHT.name
                )
        );

    // Deduplicate the names
    ds3 := TABLE(flattenedNames, {name}, name);

    RETURN ds3;
END;

/**
 * Finds the time zone records for a given location.
 *
 * @param   location        The name of the location to search for; must be a
 *                          non-empty uppercase string; REQUIRED
 * @return                  A new DATASET(STRING5 tzAbbrev, INTEGER4 secondsOffset)
 *                          containing the found records
 * @see     FindTZData
 */

EXPORT TZDataForLocation(STRING location) := FUNCTION
    ResultRec := RECORD
        STRING5         tzAbbrev;
        INTEGER4        secondsOffset;
    END;

    foundRecs := TZ_DATA(location IN locations);
    foundTrimmed := PROJECT
        (
            foundRecs,
            TRANSFORM
                (
                    ResultRec,
                    SELF := LEFT
                )
        );

    RETURN foundTrimmed;
END;

/**
 * Finds the time zone records for a given abbreviation and optional location.
 * A location should be provided as a method of differentiation if the
 * abbreviation has duplicate entries.
 *
 * @param   timeZoneAbbrev  The time zone abbreviation to search for;
 *                          must be a non-empty uppercase string; REQUIRED
 * @param   location        The name of the location to search for; if a
 *                          location is not provided or is an empty string,
 *                          all records matching only the abbreviation are
 *                          returned; OPTIONAL, defaults to an empty string
 * @return                  A new DATASET(TZDataLayout) containing the found
 *                          records
 * @see     TZDataForLocation
 */

EXPORT DATASET(TZDataLayout) FindTZData(STRING5 timeZoneAbbrev, STRING location = '') := FUNCTION
    RETURN TZ_DATA(tzAbbrev = timeZoneAbbrev AND (location = '' OR location IN locations));
END;

/**
 * Compute the offset, in seconds, between two different time zones.  Each
 * time zone is designated by a required time zone abbreviation and an
 * optional location name.  The result is the number of seconds (which can be
 * either positive or negative) that would have to be applied to a time when
 * traveling from 'fromTimeZoneAbbrev' to 'toTimeZoneAbbrev'.
 *
 * Be aware that some time zones explicitly represent daylight savings time, so
 * it is entirely possible to change not only time zones but DST observance as
 * well in a single call.
 *
 * @param   fromTimeZoneAbbrev  The time zone abbreviation designated as the
 *                              starting point; must be a non-empty uppercase
 *                              string; REQUIRED
 * @param   toTimeZoneAbbrev    The time zone abbreviation designated as the
 *                              ending point; must be a non-empty uppercase
 *                              string; REQUIRED
 * @param   fromLocation        The name of the location that goes along with
 *                              fromTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching fromTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @param   toLocation          The name of the location that goes along with
 *                              toTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching toTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @return                      The number of seconds between the two time
 *                              zones; will return zero if either time zone
 *                              cannot be found
 * @see     AdjustTimeTZ
 */

EXPORT INTEGER4 SecondsBetweenTZ(STRING5 fromTimeZoneAbbrev,
                                 STRING5 toTimeZoneAbbrev,
                                 STRING fromLocation = '',
                                 STRING toLocation = '') := FUNCTION
    fromTZ := FindTZData(fromTimeZoneAbbrev, fromLocation);
    toTZ := FindTZData(toTimeZoneAbbrev, toLocation);
    hasTZInfo := EXISTS(fromTZ) AND EXISTS(toTZ);

    fromSecondsOffset := fromTZ[1].secondsOffset;
    toSecondsOffset := toTZ[1].secondsOffset;

    RETURN IF
        (
            hasTZInfo,
            toSecondsOffset - fromSecondsOffset,
            0
        );
END;

/**
 * Adjust a given Time_t time value for another time zone.  Both the given time
 * and the destination time zone are designated by a required time zone
 * abbreviation and an optional location name.
 *
 * @param   time                The time value to adjust; REQUIRED
 * @param   fromTimeZoneAbbrev  The time zone abbreviation that the time
 *                              value is assumed to be within; must be a
 *                              non-empty uppercase string; REQUIRED
 * @param   toTimeZoneAbbrev    The time zone abbreviation designated as the
 *                              ending point; must be a non-empty uppercase
 *                              string; REQUIRED
 * @param   fromLocation        The name of the location that goes along with
 *                              fromTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching fromTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @param   toLocation          The name of the location that goes along with
 *                              toTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching toTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @return                      The given time value adjusted by the difference
 *                              between the two given time zones; if either
 *                              time zone cannot be found then the original
 *                              time value will be returned unchanged
 * @see     SecondsBetweenTZ
 */

EXPORT Time_t AdjustTimeTZ(Time_t time,
                           STRING5 fromTimeZoneAbbrev,
                           STRING5 toTimeZoneAbbrev,
                           STRING fromLocation = '',
                           STRING toLocation = '') := FUNCTION
    diff := SecondsBetweenTZ(fromTimeZoneAbbrev, toTimeZoneAbbrev, fromLocation, toLocation);
    newTime := AdjustTime(time, second_delta := diff);

    RETURN newTime;
END;

/**
 * Converts a UTC time to a time designated by a time zone abbreviation and
 * optional location.
 *
 * @param   utcTime             The UTC time value to adjust; REQUIRED
 * @param   toTimeZoneAbbrev    The time zone abbreviation designated as the
 *                              ending point; must be a non-empty uppercase
 *                              string; REQUIRED
 * @param   toLocation          The name of the location that goes along with
 *                              toTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching toTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @return                      The given UTC time value adjusted to the time
 *                              zone defined by toTimeZoneAbbrev and toLocation;
 *                              if the time zone cannot be found then the
 *                              original time value will be returned unchanged
 * @see     AdjustTimeTZ
 * @see     ToUTCTime
 */

EXPORT Time_t ToLocalTime(Time_t utcTime,
                          STRING5 toTimeZoneAbbrev,
                          STRING toLocation = '') := FUNCTION
    RETURN AdjustTimeTZ(utcTime, 'UTC', toTimeZoneAbbrev, toLocation := toLocation);
END;

/**
 * Converts a local time, defined with a time zone abbreviation and optional
 * location, to a UTC time.
 *
 * @param   localTime           The time value to adjust; REQUIRED
 * @param   fromTimeZoneAbbrev  The time zone abbreviation that the localTime
 *                              value is assumed to be within; must be a
 *                              non-empty uppercase string; REQUIRED
 * @param   fromLocation        The name of the location that goes along with
 *                              fromTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching fromTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @return                      The given local time value adjusted to UTC time;
 *                              if the given time zone cannot be found then the
 *                              original UTC time value will be returned
 *                              unchanged
 * @see     AdjustTimeTZ
 * @see     ToLocalTime
 */

EXPORT Time_t ToUTCTime(Time_t localTime,
                        STRING5 fromTimeZoneAbbrev,
                        STRING fromLocation = '') := FUNCTION
    RETURN AdjustTimeTZ(localTime, fromTimeZoneAbbrev, 'UTC', fromLocation := fromLocation);
END;

/**
 * Given a dataset that contains a time zone abbreviation and optional location,
 * this function macro appends four new attributes to the dataset that contain
 * useful information for translating a time value into another time zone.
 * This could be useful as an ETL step where time data is made common in
 * respect to one particular time zone (e.g. UTC).
 *
 * The actions within this function macro are conceptually similar to
 * SecondsBetweenTZ() but applied to an entire dataset, and somewhat more
 * efficiently.
 *
 * Note:  In order for this function macro to execute correctly, the calling
 * code must import the Std library.
 *
 * @param   inFile              The dataset to process; REQUIRED
 * @param   timeZoneAbbrevField The attribute within inFile that contains
 *                              the time zone abbreviation to use for matching;
 *                              the values in this attribute should be in
 *                              uppercase; this is not a string; REQUIRED
 * @param   newOffsetField      The attribute that will be appended to inFile
 *                              and will contain the number of seconds offset
 *                              from UTC; this is not a string; REQUIRED
 * @param   fromLocationField   The attribute within inFile that contains the
 *                              time zone location for the time zone cited by
 *                              timeZoneAbbrevField; this is not a string;
 *                              OPTIONAL, defaults to a null value (indicating
 *                              that there is no time zone location attribute)
 * @param   toTimeZoneAbbrev    The 'to' time zone abbreviation to use for all
 *                              calculations, as a string; OPTIONAL, defaults
 *                              to 'UTC'
 * @param   toLocation          The name of the location that goes along with
 *                              toTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching toTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @return                      A new dataset with the same record definition
 *                              as inFile but with four new attributes added;
 *                              the new attributes are named based on the name
 *                              given as the newOffsetField attribute:
 *                                  INTEGER4    <newOffsetField>            // Offset, in seconds, between original time zone and toTimeZoneAbbrev
 *                                  BOOLEAN     <newOffsetField>_is_valid   // TRUE if <newOffsetField> contains a valid value
 *                                  STRING5     <newOffsetField>_tz         // The value of toTimeZoneAbbrev
 *                                  STRING15    <newOffsetField>_location   // The time zone location for <newOffsetField>_tz
 *                              If <newOffsetField>_is_valid is FALSE then
 *                              <newOffsetField> will be zero.
 * @see     AppendTZAdjustedTime
 *
 * Examples:
 *
 *   ds := DATASET
 *      (
 *          [
 *              {120000, 'CT'},
 *              {120000, 'ET'}
 *          ],
 *          {Std.Date.Time_t time, STRING tz}
 *      );
 *
 *  utcOffsetDS := Std.Date.TimeZone.AppendTZOffset(ds, tz, seconds_to_utc);
 *  OUTPUT(utcOffsetDS, NAMED('offset_to_utc_result'));
 *
 *  ptOffsetDS := Std.Date.TimeZone.AppendTZOffset
 *      (
 *          ds,
 *          tz,
 *          seconds_to_pacific_time,
 *          toTimeZoneAbbrev := 'PT',
 *          toLocation := 'NORTH AMERICA'
 *      );
 *  OUTPUT(ptOffsetDS, NAMED('offset_to_pacific_time_result'));
 */

EXPORT AppendTZOffset(inFile,
                      timeZoneAbbrevField,
                      newOffsetField,
                      fromLocationField = '',
                      toTimeZoneAbbrev = '\'UTC\'',
                      toLocation = '\'\'') := FUNCTIONMACRO
    // Find the destination time zone information just once
    #UNIQUENAME(destOffsetDS);
    LOCAL %destOffsetDS% := Std.Date.TimeZone.FindTZData(toTimeZoneAbbrev, toLocation);
    #UNIQUENAME(destOffsetFound);
    LOCAL %destOffsetFound% := EXISTS(%destOffsetDS%);
    #UNIQUENAME(destLocation);
    LOCAL %destLocation% := IF(toLocation != '', toLocation, %destOffsetDS%[1].locations[1]);
    #UNIQUENAME(destOffset);
    LOCAL %destOffset% := %destOffsetDS%[1].secondsOffset;

    RETURN JOIN
        (
            inFile,
            Std.Date.TimeZone.TZ_DATA,
            LEFT.timeZoneAbbrevField = RIGHT.tzAbbrev
                #IF(#TEXT(fromLocationField) != '')
                    AND LEFT.fromLocationField IN RIGHT.locations
                #END
                AND %destOffsetFound%,
            TRANSFORM
                (
                    {
                        RECORDOF(inFile),
                        INTEGER4    newOffsetField,
                        BOOLEAN     #EXPAND(#TEXT(newOffsetField) + '_is_valid'),
                        STRING5     #EXPAND(#TEXT(newOffsetField) + '_tz'),
                        STRING15    #EXPAND(#TEXT(newOffsetField) + '_location')
                    },

                    wasFound := RIGHT.tzAbbrev != '';

                    SELF.newOffsetField := IF(wasFound, %destOffset% - RIGHT.secondsOffset, 0),
                    SELF.#EXPAND(#TEXT(newOffsetField) + '_is_valid') := wasFound,
                    SELF.#EXPAND(#TEXT(newOffsetField) + '_tz') := toTimeZoneAbbrev,
                    SELF.#EXPAND(#TEXT(newOffsetField) + '_location') := %destLocation%,
                    SELF := LEFT
                ),
            LEFT OUTER, LOOKUP
        );
ENDMACRO;

/**
 * Given a dataset that contains a time (in Time_t format), a time zone
 * abbreviation, and an optional time zone location, this function macro
 * appends four new attributes to the dataset:  A new Time_t attribute
 * containing the original time expressed in a different time zone, and three
 * attributes providing information regarding that destination time zone and
 * the validity of the translation.  This could be useful as an ETL step where
 * time data is made common in respect to one particular time zone (e.g. UTC).
 *
 * The actions within this function macro are conceptually similar to
 * AdjustTimeTZ() but applied to an entire dataset, and somewhat more
 * efficiently.
 *
 * Note:  In order for this function macro to execute correctly, the calling
 * code must import the Std library.
 *
 * @param   inFile              The dataset to process; REQUIRED
 * @param   timeField           The attribute within inFile that contains a
 *                              time represented in Time_t format; this is not
 *                              a string; REQUIRED
 * @param   timeZoneAbbrevField The attribute within inFile that contains
 *                              the time zone abbreviation to use for matching;
 *                              the values in this attribute should be in
 *                              uppercase; this is not a string; REQUIRED
 * @param   newTimeField        The attribute that will be appended to inFile
 *                              and will contain the adjusted value of timeField;
 *                              this is not a string; REQUIRED
 * @param   fromLocationField   The attribute within inFile that contains the
 *                              time zone location for the time zone cited by
 *                              timeZoneAbbrevField; this is not a string;
 *                              OPTIONAL, defaults to a null value (indicating
 *                              that there is no time zone location attribute)
 * @param   toTimeZoneAbbrev    The 'to' time zone abbreviation to use for all
 *                              calculations, as a string; OPTIONAL, defaults
 *                              to 'UTC'
 * @param   toLocation          The name of the location that goes along with
 *                              toTimeZoneAbbrev; if a location is not
 *                              provided or is an empty string, the first
 *                              record matching toTimeZoneAbbrev will be used;
 *                              OPTIONAL, defaults to an empty string
 * @return                      A new dataset with the same record definition
 *                              as inFile but with four new attributes added;
 *                              the new attributes are named based on the name
 *                              given as the newOffsetField attribute:
 *                                  Std.Date.Time_t <newOffsetField>            // Value of timeField expressed in new time zone
 *                                  BOOLEAN         <newOffsetField>_is_valid   // TRUE if <newOffsetField> contains a valid value
 *                                  STRING5         <newOffsetField>_tz         // The value of toTimeZoneAbbrev
 *                                  STRING15        <newOffsetField>_location   // The time zone location for <newOffsetField>_tz
 *                              If <newOffsetField>_is_valid is FALSE then
 *                              <newOffsetField> will have the same value as
 *                              timeField.
 * @see     AppendTZOffset
 *
 * Example:
 *
 *   ds := DATASET
 *      (
 *          [
 *              {120000, 'CT'},
 *              {120000, 'ET'}
 *          ],
 *          {Std.Date.Time_t time, STRING tz}
 *      );
 *
 *  utcRewriteDS := Std.Date.TimeZone.AppendTZAdjustedTime(ds, time, tz, utc_time);
 *  OUTPUT(utcRewriteDS, NAMED('utc_result'));
 *
 *  ptRewriteDS := Std.Date.TimeZone.AppendTZAdjustedTime
 *      (
 *          ds,
 *          time,
 *          tz,
 *          pacific_time,
 *          toTimeZoneAbbrev := 'PT',
 *          toLocation := 'NORTH AMERICA'
 *      );
 *  OUTPUT(ptRewriteDS, NAMED('pacific_time_result'));
 */

EXPORT AppendTZAdjustedTime(inFile,
                            timeField,
                            timeZoneAbbrevField,
                            newTimeField,
                            fromLocationField = '',
                            toTimeZoneAbbrev = '\'UTC\'',
                            toLocation = '\'\'') := FUNCTIONMACRO
    // Find the destination time zone information just once
    #UNIQUENAME(destOffsetDS);
    LOCAL %destOffsetDS% := Std.Date.TimeZone.FindTZData(toTimeZoneAbbrev, toLocation);
    #UNIQUENAME(destOffsetFound);
    LOCAL %destOffsetFound% := EXISTS(%destOffsetDS%);
    #UNIQUENAME(destLocation);
    LOCAL %destLocation% := IF(toLocation != '', toLocation, %destOffsetDS%[1].locations[1]);
    #UNIQUENAME(destOffset);
    LOCAL %destOffset% := %destOffsetDS%[1].secondsOffset;

    RETURN JOIN
        (
            inFile,
            Std.Date.TimeZone.TZ_DATA,
            LEFT.timeZoneAbbrevField = RIGHT.tzAbbrev
                #IF(#TEXT(fromLocationField) != '')
                    AND LEFT.fromLocationField IN RIGHT.locations
                #END
                AND %destOffsetFound%,
            TRANSFORM
                (
                    {
                        RECORDOF(inFile),
                        Std.Date.Time_t     newTimeField,
                        BOOLEAN             #EXPAND(#TEXT(newTimeField) + '_is_valid'),
                        STRING5             #EXPAND(#TEXT(newTimeField) + '_tz'),
                        STRING15            #EXPAND(#TEXT(newTimeField) + '_location')
                    },

                    wasFound := RIGHT.tzAbbrev != '';

                    SELF.newTimeField := IF
                        (
                            wasFound,
                            Std.Date.AdjustTime(LEFT.timeField, second_delta := (%destOffset% - RIGHT.secondsOffset)),
                            LEFT.timeField
                        ),
                    SELF.#EXPAND(#TEXT(newTimeField) + '_is_valid') := wasFound,
                    SELF.#EXPAND(#TEXT(newTimeField) + '_tz') := toTimeZoneAbbrev,
                    SELF.#EXPAND(#TEXT(newTimeField) + '_location') := %destLocation%,
                    SELF := LEFT
                ),
            LEFT OUTER, LOOKUP
        );
ENDMACRO;

END; // TimeZone Module

END; // Date Module
