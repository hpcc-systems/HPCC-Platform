/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

{ unsigned score; } t(namesRecord l) := TRANSFORM
    isAdult := (l.age > 25);
    adult := (integer)isAdult;
    SELF.score := l.age * CASE(adult, 0=>0.1234, 1=>0.6789, 3.14) + MAP(adult=0=>1.23,adult=1=>2.34,-99);
END;

PROJECT(namesTable, t(LEFT));
