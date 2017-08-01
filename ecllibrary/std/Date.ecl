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
 * @return                      Module with exported attributes for year, month,
 *                              day, hour, minute, second, day_of_week, date
 *                              and time.
 */

EXPORT SecondsToParts(Seconds_t seconds) := FUNCTION
    parts := ROW(TimeLib.SecondsToParts(seconds));

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
 * @param from          The first date
 * @param to            The last date
 * @return              The number of months between them.
 */

EXPORT INTEGER MonthsBetween(Date_t from, Date_t to) := FUNCTION
    fromDate := MIN(from, to);
    toDate := MAX(from, to);
    years := Year(toDate) - Year(fromDate);
    months := Month(toDate) - Month(fromDate);
    result := years * 12 + months;
    adjustedResult := result - IF(Day(fromDate) > Day(toDate), 1, 0);

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
    %y          year within century (00-99)
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
    %k          (two digits, or a space followed by a single digit)
    %M          Minute (two digits)
    %S          Second (two digits)
    %t          Whitespace
 */

EXPORT Time_t FromStringToTime(STRING time_text, VARSTRING format) :=
    StringLib.StringToTimeOfDay(time_text, format);


/**
 * Matches a string against a set of date string formats and returns a valid
 * Date_t object from the first format that successfully parses the string.
 *
 * @param date_text     The string to be converted.
 * @param formats       A set of formats to check against the string.
 *                      (See documentation for strftime)
 * @return              The date that was matched in the string.
 *                      Returns 0 if failed to match.
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
 */

EXPORT STRING ConvertDateFormat(STRING date_text, VARSTRING from_format='%m/%d/%Y', VARSTRING to_format='%Y%m%d') :=
    DateToString(FromStringToDate(date_text, from_format), to_format);


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
 * result of this function to the seconds.
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
 * @param seconds       The number seconds since epoch.
 * @return              A transform that creates a Date_rec containing the date.
 */

EXPORT Date_rec CreateDateFromSeconds(Seconds_t seconds) := TRANSFORM
    timeParts := SecondsToParts(seconds);

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
 * @param seconds       The number seconds since epoch.
 * @return              A transform that creates a Time_rec containing the time of day.
 */

EXPORT Time_rec CreateTimeFromSeconds(Seconds_t seconds) := TRANSFORM
    timeParts := SecondsToParts(seconds);

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
 * @param seconds       The number seconds since epoch.
 * @return              A transform that creates a DateTime_rec containing
 *                      date and time components.
 */

EXPORT DateTime_rec CreateDateTimeFromSeconds(Seconds_t seconds) := TRANSFORM
    timeParts := SecondsToParts(seconds);

    SELF.year := timeParts.year;
    SELF.month := timeParts.month;
    SELF.day := timeParts.day;
    SELF.hour := timeParts.hour;
    SELF.minute := timeParts.minute;
    SELF.second := timeParts.second;
END;

END; // Module
