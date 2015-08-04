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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)};
            END;

d := dataset('x',namesRecord,FLAT);

i1 := index(d, { d } ,'\\home\\person.name_first.key1');
nameIndexRecord := recordof(i1);

errorRecord := RECORD
unsigned4           code;
string50            msg;
               END;

fullRecord := RECORD(namesRecord)
errorRecord     err;
            END;

fullRecord t(nameIndexRecord l) := transform
    SELF := l;
    SELF := [];
END;

fullRecord t2(namesRecord l) := transform
    SELF := l;
    SELF := [];
END;

fullRecord createError(unsigned4 code, string50 msg) := transform
    SELF.err.code := code;
    SELF.err.msg := msg;
    SELF := [];
END;

//Simple source activity
res1a := project(i1(surname='Smith'), t(LEFT));
res1b := limit(res1a, 1000, ONFAIL(createError(99, 'Too many matching names 1')));
output(res1b);

//keyed variant
res1c := limit(res1a, 1000, ONFAIL(createError(99, 'Too many matching names 1 keyed')), keyed);
output(res1c);

//Separate limit activity
res2a := project(i1(surname='Drimbad'), t(LEFT));
res2b := nofold(sort(res2a, forename, age));
res2c := limit(res2b, 99, ONFAIL(createError(99, 'Too many matching names 2')));
output(res2c);


//Don't merge into join
res3a := join(d, i1, left.filepos = right.filepos and (right.surname='Jones'), t2(LEFT));
res3c := limit(res3a, 99, ONFAIL(createError(99, 'Too many matching names 3')));
output(res3c);


//Some aggregate versions - which reduce the projected fields amongst other issues
//Don't merge the limit in...
res4a := project(i1(surname='Pooter'), t(LEFT));
res4b := limit(res4a, 99, ONFAIL(createError(99, 'Too many matching names 4')));
output(sum(res4b, age));

res7a := project(i1(surname='Scooter'), t(LEFT));
res7b := limit(res7a, 99, ONFAIL(createError(99, 'Too many matching names 7')));
output(count(res7b));

//check transform is reduced correctly, and limit not incorrectly swapped with a project
res5a := project(i1(surname='Potter'), t(LEFT));
res5b := limit(res5a, 99, ONFAIL(createError(99, 'Too many matching names 5')));
res5c := project(res5b, transform(errorRecord, self := LEFT.err; ));
output(res5c);



//check transform is reduced correctly, and limit not incorrectly swapped with a project
res6a := project(i1(surname='Granger'), t(LEFT));
res6b := limit(res6a, 99, ONFAIL(createError(99, 'Too many matching names 5')), keyed);
res6c := project(res6b, transform(errorRecord, self := LEFT.err; ));
output(res6c);


