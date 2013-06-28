/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012-3 HPCC Systems.  All rights reserved.
############################################################################## */

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
