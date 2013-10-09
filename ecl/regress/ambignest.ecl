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

//Illustates bug #19786 - l.y is potentially ambiguous in the context of the project if only done on type.
//And yes I have seen silly code like this!

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

xNamesRecord := record
unsigned6 id;
dataset(namesRecord) matches;
        end;

xnamesTable := dataset('x',xNamesRecord,FLAT);


xnamesRecord t1(xNamesRecord l) := transform
    d := dataset(l);
    s := sort(d+d+d, id);
    xNamesRecord t2(xNamesRecord l2) := transform
            self.id := sort(l2.matches, surname, forename)[3].age;
            self := l2;
        end;
    p2 := project(s, t2(LEFT));
    self.matches := normalize(NOFOLD(p2), left.matches, transform(right));
    self := l;
    end;

p := project(nofold(xnamesTable), t1(LEFT));

output(p);
