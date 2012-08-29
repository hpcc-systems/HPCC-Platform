/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

Simple := RECORD
    STRING1 letter;
END;

ds1 := DATASET([{'A'},{'B'},{'C'},{'D'},{'A'},{'B'},{'A'},{'D'},{'D'},{'C'}], Simple);

// expect following to filter out larger groups of A & D, leaving B and C

grpd := GROUP(SORT(ds1,letter), letter);
h := HAVING(grpd, COUNT(ROWS(LEFT)) <= 2);
OUTPUT(h);
