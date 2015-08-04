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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

otherTable := dataset('other',namesRecord,FLAT);

    x := otherTable(LENGTH(TRIM(surname)) > 1);
    x2 := dedup(x, surname, all);

processLoop(dataset(namesRecord) in, unsigned c) := FUNCTION


    //Use x2 from a child query - so it IS force to a single node
    y := JOIN(in, x2, LEFT.surname = RIGHT.surname);

    RETURN y;
END;


ds1 := LOOP(namesTable, 100, LEFT.surname != x2[COUNTER].surname, processLoop(ROWS(LEFT), COUNTER));
output(ds1);
