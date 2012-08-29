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

#option ('resourceSequential', true);

personRecord := RECORD
unsigned4 personid;
string1 sex;
string33 forename;
string33 surname;
string4 yob;
unsigned4 salary;
string20 security;
unsigned2 numPublic;
unsigned1 numNotes;
    END;

person1Dataset := DATASET('person1',personRecord,FLAT);
person2Dataset := DATASET('person2',personRecord,FLAT);
person3Dataset := DATASET('person3',personRecord,FLAT);
person4Dataset := DATASET('person4',personRecord,FLAT);

boolean b1 := true : stored('b1');
boolean b2 := true : stored('b2');
boolean b3 := true : stored('b3');

o1 := output(person1Dataset);
o2 := output(person2Dataset);
o3 := output(person3Dataset);

d1 := dedup(person1Dataset, surname);
od1 := output(d1,,named('deduped1'));
d2 := d1(forename <> '');
od2 := output(d2,,named('deduped2'));

c1 := if(b1, output(person1Dataset), output(person2Dataset));
c2 := if(b2, output(person2Dataset), output(person3Dataset));
c3 := if(b3, output(person3Dataset), output(person4Dataset));

x := sequential(parallel(od1, od2, c1, c2), c3);
//x := sequential(parallel(o1, o2), o3);

y := parallel(x, output(person1Dataset+person2Dataset,,named('common')));

z := if(b1 and b2, x, y);

parallel(z, output(count(person2Dataset + person3Dataset),named('count12')));
