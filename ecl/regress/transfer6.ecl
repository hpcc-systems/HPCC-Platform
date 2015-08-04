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


phoneNumberRec := record
unsigned        code;
string8         num;
        end;

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
phoneNumberRec  p1;
phoneNumberRec  p2;
phoneNumberRec  p3;
phoneNumberRec  p4;
            END;

namesTable := dataset('x',namesRecord,FLAT);

simpleStringrec := record
string32        text;
            end;


simpleStringRec t(namesRecord l) := transform
    self.text := transfer(l, string32);
    end;

output(project(namesTable, t(LEFT)));

simpleStringRec t2(namesRecord l) := transform
    self.text := transfer(l.p1, string32);
    end;

output(project(namesTable, t2(LEFT)));


output(project(namesTable, t(row(transform(namesRecord, SELF.p1 := LEFT.p2; SELF.p2 := LEFT.p1; SELF := LEFT)))));
