/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012-13 HPCC Systems.  All rights reserved.
############################################################################## */

EXPORT FromGregorianYMD(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) := FUNCTION
    //See Frequently Asked Questions about Calendars by Claus Toendering
    a := (14 - month) DIV 12;
    y := year + YearDelta - a;
    m := month + 12*a - 3;
    jd := day + (153 * m + 2) DIV 5 + 365 * y + y DIV 4 - y DIV 100 + y DIV 400;
    RETURN jd + (GregorianDateOrigin-1);
END;
