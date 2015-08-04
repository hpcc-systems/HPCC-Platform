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

//Illustates bug #19786 - l.y is potentially ambiguous in the context of the project if only done on type.
//And yes I have seen silly code like this!

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

xNamesRecord := record
dataset(namesRecord) matches;
        end;

namesTable := dataset('x',namesRecord,FLAT);

xnamesRecord t(namesRecord l) := transform
     dids := dataset(l);
     n := normalize(dids, 10, transform(left));
     p := project(n, transform(namesRecord, self.forename := l.forename, self := left));
     self.matches := p;
     end;

p2 := project(namesTable, t(left));

output(p2);
