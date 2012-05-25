/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

strRec := { string value; };

namesRecord :=
            RECORD
string20        surname;
string10        forename;
dataset(strRec) paths;
integer2        age := 25;
            END;

namesTable1 := dataset('x1',namesRecord,FLAT);

getUniqueSet(dataset(strRec) values) := FUNCTION
    unsigned MaxPaths := 100;
    uniquePaths := DEDUP(values, value, ALL);
    RETURN IF(COUNT(uniquePaths)<MaxPaths, SET(uniquePaths, value), ['Default']);
END;

getUniqueSet2(dataset(strRec) values) := FUNCTION
    unsigned MaxPaths := 100;
    uniquePaths := DEDUP(values, value, ALL);
    limited := IF(COUNT(uniquePaths)<MaxPaths, uniquePaths, DATASET(['Default'], strRec));
    RETURN SET(limited, value);
END;

output(namesTable1(surname not in getUniqueSet(paths)));
