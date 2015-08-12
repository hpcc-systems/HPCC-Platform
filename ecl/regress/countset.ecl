/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
