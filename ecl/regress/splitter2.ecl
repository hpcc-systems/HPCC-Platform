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
            END;

//balanced split
t1 := dataset('x',namesRecord,FLAT);
s := sort(t1, surname);
c1 := s + s;
output(c1);


//unbalanced split
t2 := dataset('x2',namesRecord,FLAT);
s2 := sort(t2, surname);
c2 := s2 & s2;
output(C2);


//unbalanced split
t3 := dataset('x3',namesRecord,FLAT);
s3 := sort(t3, surname);
c3 := merge(s3, s3);
output(c3);


//balanced thor, unbalanced roxie, takes a bit of doing to force it!
t4 := dataset('x4',namesRecord,FLAT);
s4a := dedup(t4, forename);
d4b := distribute(s4a, hash(surname));
d4c := distribute(s4a, hash(forename));
s4d := sort(d4b, surname, local);
c4e := s4d & d4c;
d4f := distribute(c4e, hash(age));
s4g := sort(d4f, forename, local);
d4h := distribute(nofold(s4g), hash(surname, forename));
output(d4h);


//balanced thor, unbalanced roxie, follow the dependency..
t5 := dataset('x5', namesRecord, flat);
s5 := dedup(t5, surname);

cnt := count(s5(surname <> 'xx'));
aveAge := ave(s5(surname <> 'yy'), age);

c5 := s5(if(cnt > 100, (forename = 'John'), (age != aveAge)));
output(c5);
