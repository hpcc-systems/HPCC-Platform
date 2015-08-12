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


namesRecordEx :=
            RECORD
qstring20       surname;
qstring10       forename;
integer2        age := 0;
            END;

namesTable := dataset('x',namesRecordEx,FLAT);


namesRecordEx t1(namesrecordEx l) := transform,skip(l.surname <> '')
    self := l;
    end;

namesRecordEx t2(namesrecordEx l) := transform,skip(l.forename <> '')
    self := l;
    end;

p := project(namesTable, t1(LEFT));
p2 := project(p, t2(LEFT));

output(p2);
