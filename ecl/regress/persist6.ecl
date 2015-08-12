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
string20        forename;
integer2        age := 25;
            END;

namesRecord2 :=
            RECORD
string20        forename;
string20        surname;
integer2        age := 25;
            END;

x := dataset('x',namesRecord,FLAT);

dx := x(surname <> 'Hawthorn') : persist('storedx');

dy1 := dx(forename <> 'Gavin') : persist('dy1');
dy2 := dx(forename <> 'Jason') : persist('dy2');

dz1a := dy1(age < 10) : persist('dz1a');
dz1b := dy1(age > 20) : persist('dz1b');
dz2a := dy2(age < 10) : persist('dz2a');
dz2b := dy2(age > 20) : persist('dz2b','1000way');

count(dz1a) + count(dz1b) + count(dz2a) + count(dz2b);
